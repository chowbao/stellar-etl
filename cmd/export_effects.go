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

var effectsCmd = &cobra.Command{
	Use:   "export_effects",
	Short: "Exports the effects data over a specified range",
	Long:  "Exports the effects data over a specified range to an output file.",
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
		var transformedEffects []transform.SchemaParquet

		StreamLedgers(startNum, commonArgs.EndNum, path, commonArgs.UseCaptiveCore, env, func(seq uint32, lcm xdr.LedgerCloseMeta, outFile *os.File) {
			txReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(env.NetworkPassphrase, lcm)
			if err != nil {
				cmdLogger.LogError(fmt.Errorf("could not create transaction reader for ledger %d: %s", seq, err))
				return
			}

			lhe := txReader.GetHeader()
			for {
				tx, err := txReader.Read()
				if err == io.EOF {
					break
				}
				if err != nil {
					cmdLogger.LogError(fmt.Errorf("could not read transaction in ledger %d: %s", seq, err))
					break
				}

				attempts++
				LedgerSeq := uint32(lhe.Header.LedgerSeq)
				effects, err := transform.TransformEffect(tx, LedgerSeq, lcm, env.NetworkPassphrase)
				if err != nil {
					cmdLogger.LogError(fmt.Errorf("could not transform effects in ledger %d: %s", seq, err))
					numFailures++
					continue
				}

				for _, e := range effects {
					numBytes, err := ExportEntry(e, outFile, commonArgs.Extra)
					if err != nil {
						cmdLogger.LogError(fmt.Errorf("could not export effect in ledger %d: %s", seq, err))
						numFailures++
						continue
					}
					totalNumBytes += numBytes

					if commonArgs.WriteParquet {
						transformedEffects = append(transformedEffects, e)
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
				WriteParquet(transformedEffects, parquetPath, new(transform.EffectOutputParquet))
				MaybeUpload(cloudCredentials, cloudStorageBucket, cloudProvider, parquetPath)
			}
		}
	},
}

func init() {
	rootCmd.AddCommand(effectsCmd)
	utils.AddCommonFlags(effectsCmd.Flags())
	utils.AddArchiveFlags("effects", effectsCmd.Flags())
	utils.AddCloudStorageFlags(effectsCmd.Flags())

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
