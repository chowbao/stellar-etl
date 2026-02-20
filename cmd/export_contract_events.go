package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/stellar/stellar-etl/v2/internal/input"
	"github.com/stellar/stellar-etl/v2/internal/transform"
	"github.com/stellar/stellar-etl/v2/internal/utils"
)

var contractEventsCmd = &cobra.Command{
	Use:   "export_contract_events",
	Short: "Exports the contract events over a specified range.",
	Long:  `Exports the contract events over a specified range to an output file.`,
	Run: func(cmd *cobra.Command, args []string) {
		commonArgs, env := SetupExportCommand(cmd)
		cmdArgs := utils.MustFlags(cmd.Flags(), cmdLogger)

		transactions, err := input.GetTransactions(cmdArgs.StartNum, cmdArgs.EndNum, cmdArgs.Limit, env, cmdArgs.UseCaptiveCore)
		if err != nil {
			cmdLogger.Fatalf("could not read transactions for contract events in [%d, %d] (limit=%d): %v", cmdArgs.StartNum, cmdArgs.EndNum, cmdArgs.Limit, err)
		}

		outFile := MustOutFile(cmdArgs.Path)
		defer CloseFile(outFile)

		results := ExportResults{NumAttempts: len(transactions)}
		for _, transformInput := range transactions {
			transformed, err := transform.TransformContractEvent(transformInput.Transaction, transformInput.LedgerHistory)
			if err != nil {
				ledgerSeq := transformInput.LedgerHistory.Header.LedgerSeq
				cmdLogger.LogError(fmt.Errorf("could not transform contract events in transaction %d in ledger %d: %v", transformInput.Transaction.Index, ledgerSeq, err))
				results.NumFailures++
				continue
			}

			for _, contractEvent := range transformed {
				_, err := ExportEntry(contractEvent, outFile, cmdArgs.Extra)
				if err != nil {
					cmdLogger.LogError(fmt.Errorf("could not export contract event: %v", err))
					results.NumFailures++
					continue
				}

				if commonArgs.WriteParquet {
					results.Parquet = append(results.Parquet, contractEvent)
				}
			}
		}

		FinishExport(results, cmdArgs.Credentials, cmdArgs.Bucket, cmdArgs.Provider, cmdArgs.Path, cmdArgs.ParquetPath, commonArgs.WriteParquet, new(transform.ContractEventOutputParquet))
	},
}

func init() {
	rootCmd.AddCommand(contractEventsCmd)
	utils.AddCommonFlags(contractEventsCmd.Flags())
	utils.AddArchiveFlags("contract_events", contractEventsCmd.Flags())
	utils.AddCloudStorageFlags(contractEventsCmd.Flags())

	contractEventsCmd.MarkFlagRequired("start-ledger")
	contractEventsCmd.MarkFlagRequired("end-ledger")
}
