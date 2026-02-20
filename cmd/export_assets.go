package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/stellar/stellar-etl/v2/internal/input"
	"github.com/stellar/stellar-etl/v2/internal/transform"
	"github.com/stellar/stellar-etl/v2/internal/utils"
)

var assetsCmd = &cobra.Command{
	Use:   "export_assets",
	Short: "Exports the assets data over a specified range",
	Long:  `Exports the assets that are created from payment operations over a specified ledger range`,
	Run: func(cmd *cobra.Command, args []string) {
		commonArgs, env := SetupExportCommand(cmd)
		startNum, path, parquetPath, limit := utils.MustArchiveFlags(cmd.Flags(), cmdLogger)
		cloudStorageBucket, cloudCredentials, cloudProvider := utils.MustCloudStorageFlags(cmd.Flags(), cmdLogger)

		var paymentOps []input.AssetTransformInput
		var err error

		if commonArgs.UseCaptiveCore {
			paymentOps, err = input.GetPaymentOperationsHistoryArchive(startNum, commonArgs.EndNum, limit, env, commonArgs.UseCaptiveCore)
		} else {
			paymentOps, err = input.GetPaymentOperations(startNum, commonArgs.EndNum, limit, env, commonArgs.UseCaptiveCore)
		}
		if err != nil {
			cmdLogger.Fatalf("could not read assets in [%d, %d] (limit=%d): %v", startNum, commonArgs.EndNum, limit, err)
		}

		outFile := MustOutFile(path)
		defer CloseFile(outFile)

		// With seenIDs, the code doesn't export duplicate assets within a single export. Note that across exports, assets may be duplicated
		seenIDs := map[int64]bool{}
		results := ExportResults{NumAttempts: len(paymentOps)}
		for _, transformInput := range paymentOps {
			transformed, err := transform.TransformAsset(transformInput.Operation, transformInput.OperationIndex, transformInput.TransactionIndex, transformInput.LedgerSeqNum, transformInput.LedgerCloseMeta)
			if err != nil {
				txIndex := transformInput.TransactionIndex
				cmdLogger.LogError(fmt.Errorf("could not extract asset from operation %d in transaction %d in ledger %d: %v", transformInput.OperationIndex, txIndex, transformInput.LedgerSeqNum, err))
				results.NumFailures++
				continue
			}

			// if we have seen the asset already, do not export it
			if _, exists := seenIDs[transformed.AssetID]; exists {
				continue
			}

			seenIDs[transformed.AssetID] = true
			numBytes, err := ExportEntry(transformed, outFile, commonArgs.Extra)
			if err != nil {
				cmdLogger.LogError(fmt.Errorf("could not export asset: %v", err))
				results.NumFailures++
				continue
			}
			results.TotalNumBytes += numBytes

			if commonArgs.WriteParquet {
				results.Parquet = append(results.Parquet, transformed)
			}
		}

		FinishExport(results, cloudCredentials, cloudStorageBucket, cloudProvider, path, parquetPath, commonArgs.WriteParquet, new(transform.AssetOutputParquet))
	},
}

func init() {
	rootCmd.AddCommand(assetsCmd)
	utils.AddCommonFlags(assetsCmd.Flags())
	utils.AddArchiveFlags("assets", assetsCmd.Flags())
	utils.AddCloudStorageFlags(assetsCmd.Flags())
	assetsCmd.MarkFlagRequired("end-ledger")

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
