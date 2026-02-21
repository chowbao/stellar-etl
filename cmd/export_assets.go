package cmd

import (
	"fmt"
	"os"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stellar/stellar-etl/v2/internal/transform"
	"github.com/stellar/stellar-etl/v2/internal/utils"
)

var assetsCmd = &cobra.Command{
	Use:   "export_assets",
	Short: "Exports the assets data over a specified range",
	Long:  `Exports the assets that are created from payment operations over a specified ledger range`,
	Run: func(cmd *cobra.Command, args []string) {
		cmdLogger.SetLevel(logrus.InfoLevel)
		commonArgs := utils.MustCommonFlags(cmd.Flags(), cmdLogger)
		cmdLogger.StrictExport = commonArgs.StrictExport
		startNum, path, parquetPath, _ := utils.MustArchiveFlags(cmd.Flags(), cmdLogger)
		cloudStorageBucket, cloudCredentials, cloudProvider := utils.MustCloudStorageFlags(cmd.Flags(), cmdLogger)
		env := utils.GetEnvironmentDetails(commonArgs)

		// seenIDs deduplicates assets within a single export run; across exports, assets may be duplicated
		seenIDs := map[int64]bool{}
		numFailures := 0
		totalNumBytes := 0
		attempts := 0
		var transformedAssets []transform.SchemaParquet

		StreamLedgers(startNum, commonArgs.EndNum, path, commonArgs.UseCaptiveCore, env, func(seq uint32, lcm xdr.LedgerCloseMeta, outFile *os.File) {
			transactionSet := lcm.TransactionEnvelopes()
			for txIndex, transaction := range transactionSet {
				for opIndex, op := range transaction.Operations() {
					if op.Body.Type == xdr.OperationTypePayment || op.Body.Type == xdr.OperationTypeManageSellOffer {
						attempts++
						transformed, err := transform.TransformAsset(op, int32(opIndex), int32(txIndex), int32(seq), lcm)
						if err != nil {
							cmdLogger.LogError(fmt.Errorf("could not extract asset from operation %d in ledger %d: %s", opIndex, seq, err))
							numFailures++
							continue
						}

						if _, exists := seenIDs[transformed.AssetID]; exists {
							continue
						}
						seenIDs[transformed.AssetID] = true

						numBytes, err := ExportEntry(transformed, outFile, commonArgs.Extra)
						if err != nil {
							cmdLogger.LogError(fmt.Errorf("could not export asset in ledger %d: %s", seq, err))
							numFailures++
							continue
						}
						totalNumBytes += numBytes

						if commonArgs.WriteParquet {
							transformedAssets = append(transformedAssets, transformed)
						}
					}
				}
			}
		})

		if commonArgs.EndNum > 0 {
			cmdLogger.Infof("%d bytes written to %s", totalNumBytes, path)
			PrintTransformStats(attempts, numFailures)
			MaybeUpload(cloudCredentials, cloudStorageBucket, cloudProvider, path)
			if commonArgs.WriteParquet {
				WriteParquet(transformedAssets, parquetPath, new(transform.AssetOutputParquet))
				MaybeUpload(cloudCredentials, cloudStorageBucket, cloudProvider, parquetPath)
			}
		}
	},
}

func init() {
	rootCmd.AddCommand(assetsCmd)
	utils.AddCommonFlags(assetsCmd.Flags())
	utils.AddArchiveFlags("assets", assetsCmd.Flags())
	utils.AddCloudStorageFlags(assetsCmd.Flags())
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
