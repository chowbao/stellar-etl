package cmd

import (
	"fmt"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stellar/stellar-etl/v2/internal/input"
	"github.com/stellar/stellar-etl/v2/internal/transform"
	"github.com/stellar/stellar-etl/v2/internal/utils"
)

var diagnosticEventsCmd = &cobra.Command{
	Use:   "export_diagnostic_events",
	Short: "Exports the diagnostic events over a specified range.",
	Long:  `Exports the diagnostic and system events over a specified range to an output file.`,
	Run: func(cmd *cobra.Command, args []string) {
		cmdLogger.SetLevel(logrus.InfoLevel)
		cmdArgs := utils.MustFlags(cmd.Flags(), cmdLogger)

		// TODO: https://stellarorg.atlassian.net/browse/HUBBLE-386 GetEnvironmentDetails should be refactored
		commonArgs := utils.MustCommonFlags(cmd.Flags(), cmdLogger)
		env := utils.GetEnvironmentDetails(commonArgs)

		transactions, err := input.GetTransactions(cmdArgs.StartNum, cmdArgs.EndNum, cmdArgs.Limit, env, cmdArgs.UseCaptiveCore)
		if err != nil {
			cmdLogger.Fatal("could not read transactions: ", err)
		}

		outFile := MustOutFile(cmdArgs.Path)
		numFailures := 0
		var transformedEvents []transform.SchemaParquet
		for _, transformInput := range transactions {
			transformed, err := transform.TransformContractEvent(transformInput.Transaction, transformInput.LedgerHistory, []xdr.ContractEventType{xdr.ContractEventTypeDiagnostic, xdr.ContractEventTypeSystem})
			if err != nil {
				ledgerSeq := transformInput.LedgerHistory.Header.LedgerSeq
				cmdLogger.LogError(fmt.Errorf("could not transform diagnostic events in transaction %d in ledger %d: ", transformInput.Transaction.Index, ledgerSeq))
				numFailures += 1
				continue
			}

			for _, diagnosticEvent := range transformed {
				_, err := ExportEntry(diagnosticEvent, outFile, cmdArgs.Extra)
				if err != nil {
					cmdLogger.LogError(fmt.Errorf("could not export diagnostic event: %v", err))
					numFailures += 1
					continue
				}

				if commonArgs.WriteParquet {
					transformedEvents = append(transformedEvents, diagnosticEvent)
				}
			}

		}

		outFile.Close()

		PrintTransformStats(len(transactions), numFailures)

		MaybeUpload(cmdArgs.Credentials, cmdArgs.Bucket, cmdArgs.Provider, cmdArgs.Path)

		if commonArgs.WriteParquet {
			WriteParquet(transformedEvents, cmdArgs.ParquetPath, new(transform.ContractEventOutputParquet))
			MaybeUpload(cmdArgs.Credentials, cmdArgs.Bucket, cmdArgs.Provider, cmdArgs.ParquetPath)
		}

	},
}

func init() {
	rootCmd.AddCommand(diagnosticEventsCmd)
	utils.AddCommonFlags(diagnosticEventsCmd.Flags())
	utils.AddArchiveFlags("diagnostic_events", diagnosticEventsCmd.Flags())
	utils.AddCloudStorageFlags(diagnosticEventsCmd.Flags())

	diagnosticEventsCmd.MarkFlagRequired("start-ledger")
	diagnosticEventsCmd.MarkFlagRequired("end-ledger")
}
