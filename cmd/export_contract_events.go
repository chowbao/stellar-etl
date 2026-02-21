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

var contractEventsCmd = &cobra.Command{
	Use:   "export_contract_events",
	Short: "Exports the contract events over a specified range.",
	Long:  `Exports the contract events over a specified range to an output file.`,
	Run: func(cmd *cobra.Command, args []string) {
		cmdLogger.SetLevel(logrus.InfoLevel)
		cmdArgs := utils.MustFlags(cmd.Flags(), cmdLogger)

		// TODO: https://stellarorg.atlassian.net/browse/HUBBLE-386 GetEnvironmentDetails should be refactored
		commonArgs := utils.MustCommonFlags(cmd.Flags(), cmdLogger)
		env := utils.GetEnvironmentDetails(commonArgs)

		numFailures := 0
		attempts := 0
		var transformedEvents []transform.SchemaParquet

		StreamLedgers(cmdArgs.StartNum, cmdArgs.EndNum, cmdArgs.Path, cmdArgs.UseCaptiveCore, env, func(seq uint32, lcm xdr.LedgerCloseMeta, outFile *os.File) {
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
				events, err := transform.TransformContractEvent(tx, lhe)
				if err != nil {
					cmdLogger.LogError(fmt.Errorf("could not transform contract events in ledger %d: %s", seq, err))
					numFailures++
					continue
				}

				for _, event := range events {
					_, err = ExportEntry(event, outFile, cmdArgs.Extra)
					if err != nil {
						cmdLogger.LogError(fmt.Errorf("could not export contract event in ledger %d: %s", seq, err))
						numFailures++
					}
				}
			}
			txReader.Close()
		})

		if cmdArgs.EndNum > 0 {
			PrintTransformStats(attempts, numFailures)
			MaybeUpload(cmdArgs.Credentials, cmdArgs.Bucket, cmdArgs.Provider, cmdArgs.Path)
			if commonArgs.WriteParquet {
				WriteParquet(transformedEvents, cmdArgs.ParquetPath, new(transform.ContractEventOutputParquet))
				MaybeUpload(cmdArgs.Credentials, cmdArgs.Bucket, cmdArgs.Provider, cmdArgs.ParquetPath)
			}
		}

	},
}

func init() {
	rootCmd.AddCommand(contractEventsCmd)
	utils.AddCommonFlags(contractEventsCmd.Flags())
	utils.AddArchiveFlags("contract_events", contractEventsCmd.Flags())
	utils.AddCloudStorageFlags(contractEventsCmd.Flags())

	contractEventsCmd.MarkFlagRequired("start-ledger")
}
