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

var operationsCmd = &cobra.Command{
	Use:   "export_operations",
	Short: "Exports the operations data over a specified range",
	Long:  `Exports the operations data over a specified range. Each operation is an individual command that mutates the Stellar ledger.`,
	Run: func(cmd *cobra.Command, args []string) {
		cmdLogger.SetLevel(logrus.InfoLevel)
		commonArgs := utils.MustCommonFlags(cmd.Flags(), cmdLogger)
		cmdLogger.StrictExport = commonArgs.StrictExport
		startNum, path, parquetPath, limit := utils.MustArchiveFlags(cmd.Flags(), cmdLogger)
		cloudStorageBucket, cloudCredentials, cloudProvider := utils.MustCloudStorageFlags(cmd.Flags(), cmdLogger)
		env := utils.GetEnvironmentDetails(commonArgs)

		if commonArgs.EndNum == 0 {
			ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
			defer stop()

			backend, err := utils.CreateLedgerBackend(ctx, commonArgs.UseCaptiveCore, env)
			if err != nil {
				cmdLogger.Fatal("could not create backend: ", err)
			}

			err = backend.PrepareRange(ctx, ledgerbackend.UnboundedRange(startNum))
			if err != nil {
				cmdLogger.Fatal("could not prepare range: ", err)
			}

			outFile := MustOutFile(path)
			for seq := startNum; ctx.Err() == nil; seq++ {
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

				for {
					tx, err := txReader.Read()
					if err == io.EOF {
						break
					}
					if err != nil {
						cmdLogger.LogError(fmt.Errorf("could not read transaction in ledger %d: %s", seq, err))
						break
					}

					for index, op := range tx.Envelope.Operations() {
						transformed, err := transform.TransformOperation(op, int32(index), tx, int32(seq), lcm, env.NetworkPassphrase)
						if err != nil {
							cmdLogger.LogError(fmt.Errorf("could not transform operation %d in ledger %d: %s", index, seq, err))
							continue
						}

						_, err = ExportEntry(transformed, outFile, commonArgs.Extra)
						if err != nil {
							cmdLogger.LogError(fmt.Errorf("could not export operation in ledger %d: %s", seq, err))
						}
					}
				}
				txReader.Close()
			}
			outFile.Close()
			return
		}

		operations, err := input.GetOperations(startNum, commonArgs.EndNum, limit, env, commonArgs.UseCaptiveCore)
		if err != nil {
			cmdLogger.Fatal("could not read operations: ", err)
		}

		outFile := MustOutFile(path)
		numFailures := 0
		totalNumBytes := 0
		var transformedOps []transform.SchemaParquet
		for _, transformInput := range operations {
			transformed, err := transform.TransformOperation(transformInput.Operation, transformInput.OperationIndex, transformInput.Transaction, transformInput.LedgerSeqNum, transformInput.LedgerCloseMeta, env.NetworkPassphrase)
			if err != nil {
				txIndex := transformInput.Transaction.Index
				cmdLogger.LogError(fmt.Errorf("could not transform operation %d in transaction %d in ledger %d: %v", transformInput.OperationIndex, txIndex, transformInput.LedgerSeqNum, err))
				numFailures += 1
				continue
			}

			numBytes, err := ExportEntry(transformed, outFile, commonArgs.Extra)
			if err != nil {
				cmdLogger.LogError(fmt.Errorf("could not export operation: %v", err))
				numFailures += 1
				continue
			}
			totalNumBytes += numBytes

			if commonArgs.WriteParquet {
				transformedOps = append(transformedOps, transformed)
			}
		}

		outFile.Close()
		cmdLogger.Info("Number of bytes written: ", totalNumBytes)

		PrintTransformStats(len(operations), numFailures)

		MaybeUpload(cloudCredentials, cloudStorageBucket, cloudProvider, path)

		if commonArgs.WriteParquet {
			MaybeUpload(cloudCredentials, cloudStorageBucket, cloudProvider, parquetPath)
			WriteParquet(transformedOps, parquetPath, new(transform.OperationOutputParquet))
		}
	},
}

func init() {
	rootCmd.AddCommand(operationsCmd)
	utils.AddCommonFlags(operationsCmd.Flags())
	utils.AddArchiveFlags("operations", operationsCmd.Flags())
	utils.AddCloudStorageFlags(operationsCmd.Flags())
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
