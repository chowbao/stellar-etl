package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/stellar/stellar-etl/v2/internal/input"
	"github.com/stellar/stellar-etl/v2/internal/transform"
	"github.com/stellar/stellar-etl/v2/internal/utils"
)

var effectsCmd = &cobra.Command{
	Use:   "export_effects",
	Short: "Exports the effects data over a specified range",
	Long:  "Exports the effects data over a specified range to an output file.",
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
			LedgerSeq := uint32(transformInput.LedgerHistory.Header.LedgerSeq)
			effects, err := transform.TransformEffect(transformInput.Transaction, LedgerSeq, transformInput.LedgerCloseMeta, env.NetworkPassphrase)
			if err != nil {
				txIndex := transformInput.Transaction.Index
				cmdLogger.LogError(fmt.Errorf("could not transform effects for transaction %d in ledger %d: %v", txIndex, LedgerSeq, err))
				results.NumFailures++
				continue
			}

			for _, transformed := range effects {
				numBytes, err := ExportEntry(transformed, outFile, commonArgs.Extra)
				if err != nil {
					cmdLogger.LogError(fmt.Errorf("could not export effect: %v", err))
					results.NumFailures++
					continue
				}
				results.TotalNumBytes += numBytes

				if commonArgs.WriteParquet {
					results.Parquet = append(results.Parquet, transformed)
				}
			}
		}

		FinishExport(results, cloudCredentials, cloudStorageBucket, cloudProvider, path, parquetPath, commonArgs.WriteParquet, new(transform.EffectOutputParquet))
	},
}

func init() {
	rootCmd.AddCommand(effectsCmd)
	utils.AddCommonFlags(effectsCmd.Flags())
	utils.AddArchiveFlags("effects", effectsCmd.Flags())
	utils.AddCloudStorageFlags(effectsCmd.Flags())
	effectsCmd.MarkFlagRequired("end-ledger")

	/*
		Current flags:
			start-ledger: the ledger sequence number for the beginning of the export period
			end-ledger: the ledger sequence number for the end of the export range (required)

			limit: maximum number of effects to export; default to 6,000,000
				each transaction can have up to 100 effects
				each ledger can have up to 1000 transactions
				there are 60 new ledgers in a 5 minute period

			output-file: filename of the output file

		TODO: implement extra flags if possible
			serialize-method: the method for serialization of the output data (JSON, XDR, etc)
			start and end time as a replacement for start and end sequence numbers
	*/
}
