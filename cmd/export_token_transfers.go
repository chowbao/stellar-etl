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

var tokenTransfersCmd = &cobra.Command{
	Use:   "export_token_transfer",
	Short: "Exports the token transfer event data.",
	Long:  `Exports token transfer data within the specified range to an output file. Encodes ledgers as JSON objects and exports them to the output file.`,
	Run: func(cmd *cobra.Command, args []string) {
		cmdLogger.SetLevel(logrus.InfoLevel)
		commonArgs := utils.MustCommonFlags(cmd.Flags(), cmdLogger)
		cmdLogger.StrictExport = commonArgs.StrictExport
		startNum, path, _, _ := utils.MustArchiveFlags(cmd.Flags(), cmdLogger)
		cloudStorageBucket, cloudCredentials, cloudProvider := utils.MustCloudStorageFlags(cmd.Flags(), cmdLogger)
		env := utils.GetEnvironmentDetails(commonArgs)

		numFailures := 0
		totalNumBytes := 0
		attempts := 0

		StreamLedgers(startNum, commonArgs.EndNum, path, commonArgs.UseCaptiveCore, env, func(seq uint32, lcm xdr.LedgerCloseMeta, outFile *os.File) {
			attempts++
			transformeds, err := transform.TransformTokenTransfer(lcm, env.NetworkPassphrase)
			if err != nil {
				cmdLogger.LogError(fmt.Errorf("could not transform token transfer for ledger %d: %s", seq, err))
				numFailures++
				return
			}

			for _, t := range transformeds {
				numBytes, err := ExportEntry(t, outFile, commonArgs.Extra)
				if err != nil {
					cmdLogger.LogError(fmt.Errorf("could not export token transfer for ledger %d: %s", seq, err))
					numFailures++
					continue
				}
				totalNumBytes += numBytes
			}
		})

		if commonArgs.EndNum > 0 {
			cmdLogger.Info("Number of bytes written: ", totalNumBytes)
			PrintTransformStats(attempts, numFailures)
			MaybeUpload(cloudCredentials, cloudStorageBucket, cloudProvider, path)
		}
	},
}

func init() {
	rootCmd.AddCommand(tokenTransfersCmd)
	utils.AddCommonFlags(tokenTransfersCmd.Flags())
	utils.AddArchiveFlags("token_transfer", tokenTransfersCmd.Flags())
	utils.AddCloudStorageFlags(tokenTransfersCmd.Flags())
	/*
		Current flags:
			start-ledger: the ledger sequence number for the beginning of the export period
			end-ledger: the ledger sequence number for the end of the export range (required)

			limit: maximum number of ledgers to export; default to 60 (1 ledger per 5 seconds over our 5 minute update period)
			output-file: filename of the output file

		TODO: implement extra flags if possible
			serialize-method: the method for serialization of the output data (JSON, XDR, etc)
			start and end time as a replacement for start and end sequence numbers
	*/
}
