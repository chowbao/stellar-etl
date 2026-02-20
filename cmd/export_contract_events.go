package cmd

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/stellar-etl/v2/internal/input"
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

		if cmdArgs.EndNum == 0 {
			ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
			defer stop()

			backend, err := utils.CreateLedgerBackend(ctx, cmdArgs.UseCaptiveCore, env)
			if err != nil {
				cmdLogger.Fatal("could not create backend: ", err)
			}

			err = backend.PrepareRange(ctx, ledgerbackend.UnboundedRange(cmdArgs.StartNum))
			if err != nil {
				cmdLogger.Fatal("could not prepare range: ", err)
			}

			outFile := MustOutFile(cmdArgs.Path)
			for seq := cmdArgs.StartNum; ctx.Err() == nil; seq++ {
				lcm, err := backend.GetLedger(ctx, seq)
				if ctx.Err() != nil {
					break
				}
				if err != nil {
					cmdLogger.Fatal("could not get ledger: ", err)
				}

				txReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(env.NetworkPassphrase, lcm)
				if err != nil {
					cmdLogger.LogError(fmt.Errorf("could not create transaction reader for ledger %d: %s", seq, err))
					continue
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

					events, err := transform.TransformContractEvent(tx, lhe)
					if err != nil {
						cmdLogger.LogError(fmt.Errorf("could not transform contract events in ledger %d: %s", seq, err))
						continue
					}

					for _, event := range events {
						_, err = ExportEntry(event, outFile, cmdArgs.Extra)
						if err != nil {
							cmdLogger.LogError(fmt.Errorf("could not export contract event in ledger %d: %s", seq, err))
						}
					}
				}
				txReader.Close()
			}
			outFile.Close()
			return
		}

		transactions, err := input.GetTransactions(cmdArgs.StartNum, cmdArgs.EndNum, cmdArgs.Limit, env, cmdArgs.UseCaptiveCore)
		if err != nil {
			cmdLogger.Fatal("could not read transactions: ", err)
		}

		outFile := MustOutFile(cmdArgs.Path)
		numFailures := 0
		var transformedEvents []transform.SchemaParquet
		for _, transformInput := range transactions {
			transformed, err := transform.TransformContractEvent(transformInput.Transaction, transformInput.LedgerHistory)
			if err != nil {
				ledgerSeq := transformInput.LedgerHistory.Header.LedgerSeq
				cmdLogger.LogError(fmt.Errorf("could not transform contract events in transaction %d in ledger %d: ", transformInput.Transaction.Index, ledgerSeq))
				numFailures += 1
				continue
			}

			for _, contractEvent := range transformed {
				_, err := ExportEntry(contractEvent, outFile, cmdArgs.Extra)
				if err != nil {
					cmdLogger.LogError(fmt.Errorf("could not export contract event: %v", err))
					numFailures += 1
					continue
				}

				if commonArgs.WriteParquet {
					transformedEvents = append(transformedEvents, contractEvent)
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
	rootCmd.AddCommand(contractEventsCmd)
	utils.AddCommonFlags(contractEventsCmd.Flags())
	utils.AddArchiveFlags("contract_events", contractEventsCmd.Flags())
	utils.AddCloudStorageFlags(contractEventsCmd.Flags())

	contractEventsCmd.MarkFlagRequired("start-ledger")
}
