package cmd

import (
	"fmt"
	"io"
	"os"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stellar/stellar-etl/v2/internal/transform"
	"github.com/stellar/stellar-etl/v2/internal/utils"
)

var operationsCmd = &cobra.Command{
	Use:   "export_operations",
	Short: "Exports the operations data over a specified range",
	Long:  `Exports the operations data over a specified range. Each operation is an individual command that mutates the Stellar ledger.`,
	Run: func(cmd *cobra.Command, args []string) {
		cmdLogger.SetLevel(logrus.InfoLevel)
		commonArgs := utils.MustCommonFlags(cmd.Flags(), cmdLogger)
		cmdLogger.StrictExport = commonArgs.StrictExport
		startNum, path, parquetPath, _ := utils.MustArchiveFlags(cmd.Flags(), cmdLogger)
		cloudStorageBucket, cloudCredentials, cloudProvider := utils.MustCloudStorageFlags(cmd.Flags(), cmdLogger)
		env := utils.GetEnvironmentDetails(commonArgs)

		numFailures := 0
		totalNumBytes := 0
		attempts := 0
		var transformedOps []transform.SchemaParquet

		StreamLedgers(startNum, commonArgs.EndNum, path, commonArgs.UseCaptiveCore, env, func(seq uint32, lcm xdr.LedgerCloseMeta, outFile *os.File) {
			txReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(env.NetworkPassphrase, lcm)
			if err != nil {
				cmdLogger.LogError(fmt.Errorf("could not create transaction reader for ledger %d: %s", seq, err))
				return
			}

			for {
				tx, err := txReader.Read()
				if err == io.EOF {
					break
				}
				if err != nil {
					cmdLogger.LogError(fmt.Errorf("could not read transaction in ledger %d: %s", seq, err))
					break
				}

				for index, op := range tx.Envelope.Operations() {
					attempts++
					transformed, err := transform.TransformOperation(op, int32(index), tx, int32(seq), lcm, env.NetworkPassphrase)
					if err != nil {
						cmdLogger.LogError(fmt.Errorf("could not transform operation %d in ledger %d: %s", index, seq, err))
						numFailures++
						continue
					}

					numBytes, err := ExportEntry(transformed, outFile, commonArgs.Extra)
					if err != nil {
						cmdLogger.LogError(fmt.Errorf("could not export operation in ledger %d: %s", seq, err))
						numFailures++
						continue
					}
					totalNumBytes += numBytes

					if commonArgs.WriteParquet {
						transformedOps = append(transformedOps, transformed)
					}
				}
			}
			txReader.Close()
		})

		if commonArgs.EndNum > 0 {
			cmdLogger.Info("Number of bytes written: ", totalNumBytes)
			PrintTransformStats(attempts, numFailures)
			MaybeUpload(cloudCredentials, cloudStorageBucket, cloudProvider, path)
			if commonArgs.WriteParquet {
				MaybeUpload(cloudCredentials, cloudStorageBucket, cloudProvider, parquetPath)
				WriteParquet(transformedOps, parquetPath, new(transform.OperationOutputParquet))
			}
		}
	},
}

func init() {
	rootCmd.AddCommand(operationsCmd)
	utils.AddCommonFlags(operationsCmd.Flags())
	utils.AddArchiveFlags("operations", operationsCmd.Flags())
	utils.AddCloudStorageFlags(operationsCmd.Flags())
	/*
		Current flags:
			start-ledger: the ledger sequence number for the beginning of the export period
			end-ledger: the ledger sequence number for the end of the export range (required)

			limit: maximum number of operations to export; default to 6,000,000
				each transaction can have up to 100 operations
				each ledger can have up to 1000 transactions
				there are 60 new ledgers in a 5 minute period

			output-file: filename of the output file

		TODO: implement extra flags if possible
			serialize-method: the method for serialization of the output data (JSON, XDR, etc)
			start and end time as a replacement for start and end sequence numbers
	*/
}
