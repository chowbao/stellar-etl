package utils

import (
	"github.com/spf13/pflag"
)

// AddCommonFlags adds the flags common to all commands: start-ledger, end-ledger, stdout, and strict-export
func AddCommonFlags(flags *pflag.FlagSet) {
	flags.Uint32P("end-ledger", "e", 0, "The ledger sequence number for the end of the export range")
	flags.Bool("strict-export", false, "If set, transform errors will be fatal.")
	flags.Bool("testnet", false, "If set, will connect to Testnet instead of Mainnet.")
	flags.Bool("futurenet", false, "If set, will connect to Futurenet instead of Mainnet.")
	flags.StringToStringP("extra-fields", "u", map[string]string{}, "Additional fields to append to output jsons. Used for appending metadata")
	flags.Bool("captive-core", false, "(Deprecated; Will be removed in the Protocol 23 update) If set, run captive core to retrieve data. Otherwise use TxMeta file datastore.")
	// TODO: This should be changed back to sdf-ledger-close-meta/ledgers when P23 is released and data lake is updated
	flags.String("datastore-path", "sdf-ledger-close-meta/v1/ledgers", "Datastore bucket path to read txmeta files from.")
	flags.Uint32("buffer-size", 200, "Buffer size sets the max limit for the number of txmeta files that can be held in memory.")
	flags.Uint32("num-workers", 10, "Number of workers to spawn that read txmeta files from the datastore.")
	flags.Uint32("retry-limit", 3, "Datastore GetLedger retry limit.")
	flags.Uint32("retry-wait", 5, "Time in seconds to wait for GetLedger retry.")
	flags.Bool("write-parquet", false, "If set, write output as parquet files.")
}

// AddArchiveFlags adds the history archive specific flags: output, and limit
// TODO: https://stellarorg.atlassian.net/browse/HUBBLE-386 Rename AddArchiveFlags to something more relevant
func AddArchiveFlags(objectName string, flags *pflag.FlagSet) {
	flags.Uint32P("start-ledger", "s", 2, "The ledger sequence number for the beginning of the export period. Defaults to genesis ledger")
	flags.StringP("output", "o", "exported_"+objectName+".txt", "Filename of the output file")
	flags.String("parquet-output", "exported_"+objectName+".parquet", "Filename of the parquet output file")
	flags.Int64P("limit", "l", -1, "Maximum number of "+objectName+" to export. If the limit is set to a negative number, all the objects in the provided range are exported")
}

// AddCloudStorageFlags adds the cloud storage releated flags: cloud-storage-bucket, cloud-credentials
func AddCloudStorageFlags(flags *pflag.FlagSet) {
	flags.String("cloud-storage-bucket", "stellar-etl-cli", "Cloud storage bucket to export to.")
	flags.String("cloud-credentials", "", "Path to cloud provider service account credentials. Only used for local/dev purposes. "+
		"When run on GCP, credentials should be inferred by service account json.")
	flags.String("cloud-provider", "", "Cloud provider for storage services.")
}

// AddCoreFlags adds the captive core specific flags: core-executable, core-config, batch-size, and output flags
// TODO: https://stellarorg.atlassian.net/browse/HUBBLE-386 Deprecate?
func AddCoreFlags(flags *pflag.FlagSet, defaultFolder string) {
	flags.StringP("core-executable", "x", "", "Filepath to the stellar-core executable")
	flags.StringP("core-config", "c", "", "Filepath to the config file for stellar-core")

	flags.Uint32P("batch-size", "b", 64, "number of ledgers to export changes from in each batches")
	// TODO: https://stellarorg.atlassian.net/browse/HUBBLE-386 Move output to different flag group
	flags.StringP("output", "o", defaultFolder, "Folder that will contain the output files")
	flags.String("parquet-output", defaultFolder, "Folder that will contain the parquet output files")

	flags.Uint32P("start-ledger", "s", 2, "The ledger sequence number for the beginning of the export period. Defaults to genesis ledger")
}

// AddExportTypeFlags adds the captive core specifc flags: export-{type} flags
func AddExportTypeFlags(flags *pflag.FlagSet) {
	flags.BoolP("export-accounts", "a", false, "set in order to export account changes")
	flags.BoolP("export-trustlines", "t", false, "set in order to export trustline changes")
	flags.BoolP("export-offers", "f", false, "set in order to export offer changes")
	flags.BoolP("export-pools", "p", false, "set in order to export liquidity pool changes")
	flags.BoolP("export-balances", "l", false, "set in order to export claimable balance changes")
	flags.BoolP("export-contract-code", "", false, "set in order to export contract code changes")
	flags.BoolP("export-contract-data", "", false, "set in order to export contract data changes")
	flags.BoolP("export-config-settings", "", false, "set in order to export config settings changes")
	flags.BoolP("export-ttl", "", false, "set in order to export ttl changes")
	flags.BoolP("export-restored-keys", "", false, "set in order to export restored ledger keys")
}

// TODO: https://stellarorg.atlassian.net/browse/HUBBLE-386 better flags/params
// Some flags should be named better
type FlagValues struct {
	StartNum       uint32
	EndNum         uint32
	StrictExport   bool
	IsTest         bool
	IsFuture       bool
	Extra          map[string]string
	UseCaptiveCore bool
	DatastorePath  string
	BufferSize     uint32
	NumWorkers     uint32
	RetryLimit     uint32
	RetryWait      uint32
	Path           string
	ParquetPath    string
	Limit          int64
	Bucket         string
	Credentials    string
	Provider       string
	WriteParquet   bool
}

// MustFlags gets the values of the the flags for all commands.
// If any do not exist, it stops the program fatally using the logger
// TODO: https://stellarorg.atlassian.net/browse/HUBBLE-386 Not sure if all these arg checks are necessary
func MustFlags(flags *pflag.FlagSet, logger *EtlLogger) FlagValues {
	endNum, err := flags.GetUint32("end-ledger")
	if err != nil {
		logger.Fatal("could not get end sequence number: ", err)
	}

	strictExport, err := flags.GetBool("strict-export")
	if err != nil {
		logger.Fatal("could not get strict-export boolean: ", err)
	}

	isTest, err := flags.GetBool("testnet")
	if err != nil {
		logger.Fatal("could not get testnet boolean: ", err)
	}

	isFuture, err := flags.GetBool("futurenet")
	if err != nil {
		logger.Fatal("could not get futurenet boolean: ", err)
	}

	extra, err := flags.GetStringToString("extra-fields")
	if err != nil {
		logger.Fatal("could not get extra fields string: ", err)
	}

	useCaptiveCore, err := flags.GetBool("captive-core")
	if err != nil {
		logger.Fatal("could not get captive-core flag: ", err)
	}
	// Deprecation warning
	if useCaptiveCore {
		logger.Warn("warning: the option to run with captive-core will be deprecated in the Protocol 23 update")
	}

	datastorePath, err := flags.GetString("datastore-path")
	if err != nil {
		logger.Fatal("could not get datastore-bucket-path string: ", err)
	}

	bufferSize, err := flags.GetUint32("buffer-size")
	if err != nil {
		logger.Fatal("could not get buffer-size uint32: ", err)
	}

	numWorkers, err := flags.GetUint32("num-workers")
	if err != nil {
		logger.Fatal("could not get num-workers uint32: ", err)
	}

	retryLimit, err := flags.GetUint32("retry-limit")
	if err != nil {
		logger.Fatal("could not get retry-limit uint32: ", err)
	}

	retryWait, err := flags.GetUint32("retry-wait")
	if err != nil {
		logger.Fatal("could not get retry-wait uint32: ", err)
	}

	startNum, err := flags.GetUint32("start-ledger")
	if err != nil {
		logger.Fatal("could not get start sequence number: ", err)
	}

	path, err := flags.GetString("output")
	if err != nil {
		logger.Fatal("could not get output filename: ", err)
	}

	parquetPath, err := flags.GetString("parquet-output")
	if err != nil {
		logger.Fatal("could not get parquet-output filename: ", err)
	}

	limit, err := flags.GetInt64("limit")
	if err != nil {
		logger.Fatal("could not get limit: ", err)
	}

	bucket, err := flags.GetString("cloud-storage-bucket")
	if err != nil {
		logger.Fatal("could not get cloud storage bucket: ", err)
	}

	credentials, err := flags.GetString("cloud-credentials")
	if err != nil {
		logger.Fatal("could not get cloud credentials file: ", err)
	}

	provider, err := flags.GetString("cloud-provider")
	if err != nil {
		logger.Fatal("could not get cloud provider: ", err)
	}

	WriteParquet, err := flags.GetBool("write-parquet")
	if err != nil {
		logger.Fatal("could not get write-parquet flag: ", err)
	}

	return FlagValues{
		StartNum:       startNum,
		EndNum:         endNum,
		StrictExport:   strictExport,
		IsTest:         isTest,
		IsFuture:       isFuture,
		Extra:          extra,
		UseCaptiveCore: useCaptiveCore,
		DatastorePath:  datastorePath,
		BufferSize:     bufferSize,
		NumWorkers:     numWorkers,
		RetryLimit:     retryLimit,
		RetryWait:      retryWait,
		Path:           path,
		ParquetPath:    parquetPath,
		Limit:          limit,
		Bucket:         bucket,
		Credentials:    credentials,
		Provider:       provider,
		WriteParquet:   WriteParquet,
	}
}

type CommonFlagValues struct {
	EndNum         uint32
	StrictExport   bool
	IsTest         bool
	IsFuture       bool
	Extra          map[string]string
	UseCaptiveCore bool
	DatastorePath  string
	BufferSize     uint32
	NumWorkers     uint32
	RetryLimit     uint32
	RetryWait      uint32
	WriteParquet   bool
}

// MustCommonFlags gets the values of the the flags common to all commands: end-ledger and strict-export.
// If any do not exist, it stops the program fatally using the logger
func MustCommonFlags(flags *pflag.FlagSet, logger *EtlLogger) CommonFlagValues {
	endNum, err := flags.GetUint32("end-ledger")
	if err != nil {
		logger.Fatal("could not get end sequence number: ", err)
	}

	strictExport, err := flags.GetBool("strict-export")
	if err != nil {
		logger.Fatal("could not get strict-export boolean: ", err)
	}

	isTest, err := flags.GetBool("testnet")
	if err != nil {
		logger.Fatal("could not get testnet boolean: ", err)
	}

	isFuture, err := flags.GetBool("futurenet")
	if err != nil {
		logger.Fatal("could not get futurenet boolean: ", err)
	}

	extra, err := flags.GetStringToString("extra-fields")
	if err != nil {
		logger.Fatal("could not get extra fields string: ", err)
	}

	useCaptiveCore, err := flags.GetBool("captive-core")
	if err != nil {
		logger.Fatal("could not get captive-core flag: ", err)
	}
	if useCaptiveCore {
		logger.Warn("warning: the option to run with captive-core will be deprecated in the Protocol 23 update")
	}

	datastorePath, err := flags.GetString("datastore-path")
	if err != nil {
		logger.Fatal("could not get datastore-bucket-path string: ", err)
	}

	bufferSize, err := flags.GetUint32("buffer-size")
	if err != nil {
		logger.Fatal("could not get buffer-size uint32: ", err)
	}

	numWorkers, err := flags.GetUint32("num-workers")
	if err != nil {
		logger.Fatal("could not get num-workers uint32: ", err)
	}

	retryLimit, err := flags.GetUint32("retry-limit")
	if err != nil {
		logger.Fatal("could not get retry-limit uint32: ", err)
	}

	retryWait, err := flags.GetUint32("retry-wait")
	if err != nil {
		logger.Fatal("could not get retry-wait uint32: ", err)
	}

	WriteParquet, err := flags.GetBool("write-parquet")
	if err != nil {
		logger.Fatal("could not get write-parquet flag: ", err)
	}

	return CommonFlagValues{
		EndNum:         endNum,
		StrictExport:   strictExport,
		IsTest:         isTest,
		IsFuture:       isFuture,
		Extra:          extra,
		UseCaptiveCore: useCaptiveCore,
		DatastorePath:  datastorePath,
		BufferSize:     bufferSize,
		NumWorkers:     numWorkers,
		RetryLimit:     retryLimit,
		RetryWait:      retryWait,
		WriteParquet:   WriteParquet,
	}
}

// MustArchiveFlags gets the values of the the history archive specific flags: start-ledger, output, and limit
func MustArchiveFlags(flags *pflag.FlagSet, logger *EtlLogger) (startNum uint32, path string, parquetPath string, limit int64) {
	startNum, err := flags.GetUint32("start-ledger")
	if err != nil {
		logger.Fatal("could not get start sequence number: ", err)
	}

	path, err = flags.GetString("output")
	if err != nil {
		logger.Fatal("could not get output filename: ", err)
	}

	parquetPath, err = flags.GetString("parquet-output")
	if err != nil {
		logger.Fatal("could not get parquet-output filename: ", err)
	}

	limit, err = flags.GetInt64("limit")
	if err != nil {
		logger.Fatal("could not get limit: ", err)
	}

	return
}

// MustBucketFlags gets the values of the bucket list specific flags: output
func MustBucketFlags(flags *pflag.FlagSet, logger *EtlLogger) (path string) {
	path, err := flags.GetString("output")
	if err != nil {
		logger.Fatal("could not get output filename: ", err)
	}

	return
}

// MustCloudStorageFlags gets the values of the bucket list specific flags: cloud-storage-bucket, cloud-credentials
func MustCloudStorageFlags(flags *pflag.FlagSet, logger *EtlLogger) (bucket, credentials, provider string) {
	bucket, err := flags.GetString("cloud-storage-bucket")
	if err != nil {
		logger.Fatal("could not get cloud storage bucket: ", err)
	}

	credentials, err = flags.GetString("cloud-credentials")
	if err != nil {
		logger.Fatal("could not get cloud credentials file: ", err)
	}

	provider, err = flags.GetString("cloud-provider")
	if err != nil {
		logger.Fatal("could not get cloud provider: ", err)
	}

	return
}

// MustCoreFlags gets the values for the core-executable, core-config, start ledger batch-size, and output flags. If any do not exist, it stops the program fatally using the logger
func MustCoreFlags(flags *pflag.FlagSet, logger *EtlLogger) (execPath, configPath string, startNum, batchSize uint32, path, parquetPath string) {
	execPath, err := flags.GetString("core-executable")
	if err != nil {
		logger.Fatal("could not get path to stellar-core executable, which is mandatory when not starting at the genesis ledger (ledger 1): ", err)
	}

	configPath, err = flags.GetString("core-config")
	if err != nil {
		logger.Fatal("could not get path to stellar-core config file, is mandatory when not starting at the genesis ledger (ledger 1): ", err)
	}

	path, err = flags.GetString("output")
	if err != nil {
		logger.Fatal("could not get output filename: ", err)
	}

	parquetPath, err = flags.GetString("parquet-output")
	if err != nil {
		logger.Fatal("could not get output filename: ", err)
	}

	startNum, err = flags.GetUint32("start-ledger")
	if err != nil {
		logger.Fatal("could not get start sequence number: ", err)
	}

	batchSize, err = flags.GetUint32("batch-size")
	if err != nil {
		logger.Fatal("could not get batch size: ", err)
	}

	return
}

// MustExportTypeFlags gets the values for the export-accounts, export-offers, and export-trustlines flags. If any do not exist, it stops the program fatally using the logger
// func MustExportTypeFlags(flags *pflag.FlagSet, logger *EtlLogger) (exportAccounts, exportOffers, exportTrustlines, exportPools, exportBalances, exportContractCode, exportContractData, exportConfigSettings, exportTtl bool) {
func MustExportTypeFlags(flags *pflag.FlagSet, logger *EtlLogger) map[string]bool {
	var err error
	exports := map[string]bool{
		"export-accounts":        false,
		"export-trustlines":      false,
		"export-offers":          false,
		"export-pools":           false,
		"export-balances":        false,
		"export-contract-code":   false,
		"export-contract-data":   false,
		"export-config-settings": false,
		"export-ttl":             false,
		"export-restored-keys":   false,
	}

	// Check if any flag was explicitly set to true
	anyTrue := false
	for export_name := range exports {
		exports[export_name], err = flags.GetBool(export_name)
		if err != nil {
			logger.Fatalf("could not get %s flag: %v", export_name, err)
		}
		if exports[export_name] {
			anyTrue = true
		}
	}

	// If no flags were set to true, export everything
	if !anyTrue {
		for export_name := range exports {
			exports[export_name] = true
		}
	}

	return exports
}
