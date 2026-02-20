package cmd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/stellar/stellar-etl/v2/internal/transform"
	"github.com/stellar/stellar-etl/v2/internal/utils"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/writer"
)

type CloudStorage interface {
	UploadTo(credentialsPath, bucket, path string) error
}

func createOutputFile(filepath string) error {
	var _, err = os.Stat(filepath)
	if os.IsNotExist(err) {
		var _, err = os.Create(filepath)
		if err != nil {
			return err
		}
	}

	return nil
}

func MustOutFile(path string) *os.File {
	absolutePath, err := filepath.Abs(path)
	if err != nil {
		cmdLogger.Fatal("could not get absolute filepath: ", err)
	}

	err = os.MkdirAll(filepath.Dir(path), os.ModePerm)
	if err != nil {
		cmdLogger.Fatalf("could not create directory %s: %s", path, err)
	}

	err = createOutputFile(absolutePath)
	if err != nil {
		cmdLogger.Fatal("could not create output file: ", err)
	}

	outFile, err := os.OpenFile(absolutePath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		cmdLogger.Fatal("error in opening output file: ", err)
	}

	return outFile
}

func ExportEntry(entry interface{}, outFile *os.File, extra map[string]string) (int, error) {
	// This extra marshalling/unmarshalling is silly, but it's required to properly handle the null.[String|Int*] types, and add the extra fields.
	m, err := json.Marshal(entry)
	if err != nil {
		return 0, fmt.Errorf("error marshalling entry: %v", err)
	}
	i := map[string]interface{}{}
	// Use a decoder here so that 'UseNumber' ensures large ints are properly decoded
	decoder := json.NewDecoder(bytes.NewReader(m))
	decoder.UseNumber()
	if err = decoder.Decode(&i); err != nil {
		return 0, fmt.Errorf("error unmarshalling entry: %v", err)
	}
	for k, v := range extra {
		i[k] = v
	}

	marshalled, err := json.Marshal(i)
	if err != nil {
		return 0, fmt.Errorf("could not json encode entry: %v", err)
	}
	cmdLogger.Debugf("Writing entry to %s", outFile.Name())
	numBytes, err := outFile.Write(marshalled)
	if err != nil {
		return 0, fmt.Errorf("error writing entry to file: %v", err)
	}
	newLineNumBytes, err := outFile.WriteString("\n")
	if err != nil {
		return 0, fmt.Errorf("error writing newline to file %s: %v", outFile.Name(), err)
	}
	return numBytes + newLineNumBytes, nil
}

// SetupExportCommand initializes logging from the command's flags and returns common flag values and environment details.
func SetupExportCommand(cmd *cobra.Command) (utils.CommonFlagValues, utils.EnvironmentDetails) {
	cmdLogger.SetLevel(logrus.InfoLevel)
	commonArgs := utils.MustCommonFlags(cmd.Flags(), cmdLogger)
	cmdLogger.StrictExport = commonArgs.StrictExport
	env := utils.GetEnvironmentDetails(commonArgs)
	return commonArgs, env
}

// CloseFile closes a file and logs any error encountered.
func CloseFile(f *os.File) {
	if err := f.Close(); err != nil {
		cmdLogger.Errorf("error closing file %s: %v", f.Name(), err)
	}
}

// ExportResults holds the common output state from an export loop.
type ExportResults struct {
	NumAttempts   int
	NumFailures   int
	TotalNumBytes int
	Parquet       []transform.SchemaParquet
}

// FinishExport handles the common post-loop work: logging byte count,
// printing transform stats, uploading the JSON file, and optionally
// writing + uploading the parquet file.
func FinishExport(results ExportResults, cloudCredentials, cloudStorageBucket, cloudProvider, path, parquetPath string, writeParquet bool, parquetSchema interface{}) {
	cmdLogger.Info("Number of bytes written: ", results.TotalNumBytes)
	PrintTransformStats(results.NumAttempts, results.NumFailures)
	MaybeUpload(cloudCredentials, cloudStorageBucket, cloudProvider, path)

	if writeParquet && parquetSchema != nil {
		WriteParquet(results.Parquet, parquetPath, parquetSchema)
		MaybeUpload(cloudCredentials, cloudStorageBucket, cloudProvider, parquetPath)
	}
}

// Prints the number of attempted, failed, and successful transformations as a JSON object
func PrintTransformStats(attempts, failures int) {
	resultsMap := map[string]int{
		"attempted_transforms":  attempts,
		"failed_transforms":     failures,
		"successful_transforms": attempts - failures,
	}

	results, err := json.Marshal(resultsMap)
	if err != nil {
		cmdLogger.Fatal("Could not marshal results: ", err)
	}

	cmdLogger.Info(string(results))
}

func exportFilename(start, end uint32, dataType string) string {
	return fmt.Sprintf("%d-%d-%s.txt", start, end-1, dataType)
}

func exportParquetFilename(start, end uint32, dataType string) string {
	return fmt.Sprintf("%d-%d-%s.parquet", start, end-1, dataType)
}

func deleteLocalFiles(path string) error {
	err := os.RemoveAll(path)
	if err != nil {
		cmdLogger.Errorf("Unable to remove %s: %s", path, err)
		return err
	}
	cmdLogger.Infof("Successfully deleted %s", path)
	return nil
}

func MaybeUpload(cloudCredentials, cloudStorageBucket, cloudProvider, path string) {
	if cloudProvider == "" {
		cmdLogger.Info("No cloud provider specified for upload. Skipping upload.")
		return
	}

	if len(cloudStorageBucket) == 0 {
		cmdLogger.Fatal("No bucket specified")
		return
	}

	var cloudStorage CloudStorage
	switch cloudProvider {
	case "gcp":
		cloudStorage = newGCS(cloudCredentials, cloudStorageBucket)
		err := cloudStorage.UploadTo(cloudCredentials, cloudStorageBucket, path)
		if err != nil {
			cmdLogger.Fatalf("Unable to upload output to GCS: %s", err)
			return
		}
	default:
		cmdLogger.Fatal("Unknown cloud provider")
	}
}

// WriteParquet creates the parquet file and writes the exported data into it.
//
// Parameters:
//
//	data []transform.SchemaParquet  - The slice of data to be written to the Parquet file.
//										SchemaParquet is an interface used to call ToParquet()
//										which is defined for each schema/export.
//	path string                     - The file path where the Parquet file will be created and written.
//										For example, "some/file/path/export_output.parquet"
//	schema interface{}              - The schema that defines the structure of the Parquet file.
//
//	Errors:
//
//	stellar-etl will log a Fatal error and stop in the case it cannot create or write to the parquet file
func WriteParquet(data []transform.SchemaParquet, path string, schema interface{}) {
	parquetFile, err := local.NewLocalFileWriter(path)
	if err != nil {
		cmdLogger.Fatal("could not create parquet file: ", err)
	}
	defer parquetFile.Close()

	writer, err := writer.NewParquetWriter(parquetFile, schema, 1)
	if err != nil {
		cmdLogger.Fatal("could not create parquet file writer: ", err)
	}
	defer writer.WriteStop()

	for _, record := range data {
		if err := writer.Write(record.ToParquet()); err != nil {
			cmdLogger.Fatal("could not write record to parquet file: ", err)
		}
	}
}
