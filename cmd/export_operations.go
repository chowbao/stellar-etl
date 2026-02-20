package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/stellar/stellar-etl/v2/internal/input"
	"github.com/stellar/stellar-etl/v2/internal/transform"
	"github.com/stellar/stellar-etl/v2/internal/utils"
)

var operationsCmd = &cobra.Command{
	Use:   "export_operations",
	Short: "Exports the operations data over a specified range",
	Long:  `Exports the operations data over a specified range. Each operation is an individual command that mutates the Stellar ledger.`,
	Run: func(cmd *cobra.Command, args []string) {
		commonArgs, env := SetupExportCommand(cmd)
		startNum, path, parquetPath, limit := utils.MustArchiveFlags(cmd.Flags(), cmdLogger)
		cloudStorageBucket, cloudCredentials, cloudProvider := utils.MustCloudStorageFlags(cmd.Flags(), cmdLogger)

		operations, err := input.GetOperations(startNum, commonArgs.EndNum, limit, env, commonArgs.UseCaptiveCore)
		if err != nil {
			cmdLogger.Fatalf("could not read operations in [%d, %d] (limit=%d): %v", startNum, commonArgs.EndNum, limit, err)
		}

		outFile := MustOutFile(path)
		defer CloseFile(outFile)

		results := ExportResults{NumAttempts: len(operations)}
		for _, transformInput := range operations {
			transformed, err := transform.TransformOperation(transformInput.Operation, transformInput.OperationIndex, transformInput.Transaction, transformInput.LedgerSeqNum, transformInput.LedgerCloseMeta, env.NetworkPassphrase)
			if err != nil {
				txIndex := transformInput.Transaction.Index
				cmdLogger.LogError(fmt.Errorf("could not transform operation %d in transaction %d in ledger %d: %v", transformInput.OperationIndex, txIndex, transformInput.LedgerSeqNum, err))
				results.NumFailures++
				continue
			}

			numBytes, err := ExportEntry(transformed, outFile, commonArgs.Extra)
			if err != nil {
				cmdLogger.LogError(fmt.Errorf("could not export operation: %v", err))
				results.NumFailures++
				continue
			}
			results.TotalNumBytes += numBytes

			if commonArgs.WriteParquet {
				results.Parquet = append(results.Parquet, transformed)
			}
		}

		FinishExport(results, cloudCredentials, cloudStorageBucket, cloudProvider, path, parquetPath, commonArgs.WriteParquet, new(transform.OperationOutputParquet))
	},
}

func init() {
	rootCmd.AddCommand(operationsCmd)
	utils.AddCommonFlags(operationsCmd.Flags())
	utils.AddArchiveFlags("operations", operationsCmd.Flags())
	utils.AddCloudStorageFlags(operationsCmd.Flags())
	operationsCmd.MarkFlagRequired("end-ledger")

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
