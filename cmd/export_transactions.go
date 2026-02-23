package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/stellar/stellar-etl/v2/internal/input"
	"github.com/stellar/stellar-etl/v2/internal/transform"
	"github.com/stellar/stellar-etl/v2/internal/utils"
)

var transactionsCmd = &cobra.Command{
	Use:   "export_transactions",
	Short: "Exports the transaction data over a specified range.",
	Long:  `Exports the transaction data over a specified range to an output file.`,
	Run: func(cmd *cobra.Command, args []string) {
		commonArgs, env := SetupExportCommand(cmd)
		startNum, path, parquetPath, limit := utils.MustArchiveFlags(cmd.Flags(), cmdLogger)
		cloudStorageBucket, cloudCredentials, cloudProvider := utils.MustCloudStorageFlags(cmd.Flags(), cmdLogger)

		transactions, err := input.GetTransactions(startNum, commonArgs.EndNum, limit, env, commonArgs.UseCaptiveCore)
		if err != nil {
			cmdLogger.Fatalf("could not read transactions in [%d, %d] (limit=%d): %v", startNum, commonArgs.EndNum, limit, err)
		}

		outFile := MustOutFile(path)
		defer CloseFile(outFile)

		results := ExportResults{NumAttempts: len(transactions)}
		for _, transformInput := range transactions {
			transformed, err := transform.TransformTransaction(transformInput.Transaction, transformInput.LedgerHistory)
			if err != nil {
				ledgerSeq := transformInput.LedgerHistory.Header.LedgerSeq
				cmdLogger.LogError(fmt.Errorf("could not transform transaction %d in ledger %d: %v", transformInput.Transaction.Index, ledgerSeq, err))
				results.NumFailures++
				continue
			}

			numBytes, err := ExportEntry(transformed, outFile, commonArgs.Extra)
			if err != nil {
				cmdLogger.LogError(fmt.Errorf("could not export transaction: %v", err))
				results.NumFailures++
				continue
			}
			results.TotalNumBytes += numBytes

			if commonArgs.WriteParquet {
				results.Parquet = append(results.Parquet, transformed)
			}
		}

		FinishExport(results, cloudCredentials, cloudStorageBucket, cloudProvider, path, parquetPath, commonArgs.WriteParquet, new(transform.TransactionOutputParquet))
	},
}

func init() {
	rootCmd.AddCommand(transactionsCmd)
	utils.AddCommonFlags(transactionsCmd.Flags())
	utils.AddArchiveFlags("transactions", transactionsCmd.Flags())
	utils.AddCloudStorageFlags(transactionsCmd.Flags())
	transactionsCmd.MarkFlagRequired("end-ledger")

	/*
		Current flags:
			start-ledger: the ledger sequence number for the beginning of the export period
			end-ledger: the ledger sequence number for the end of the export range (*required)

			limit: maximum number of transactions to export
				TODO: measure a good default value that ensures all transactions within a 5 minute period will be exported with a single call
				The current max_tx_set_size is 1000 and there are 60 new ledgers in a 5 minute period:
					1000*60 = 60000

			output-file: filename of the output file

		TODO: implement extra flags if possible
			serialize-method: the method for serialization of the output data (JSON, XDR, etc)
			start and end time as a replacement for start and end sequence numbers
	*/
}
