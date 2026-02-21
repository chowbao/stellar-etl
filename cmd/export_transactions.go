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

var transactionsCmd = &cobra.Command{
	Use:   "export_transactions",
	Short: "Exports the transaction data over a specified range.",
	Long:  `Exports the transaction data over a specified range to an output file.`,
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
		var transformedTransaction []transform.SchemaParquet

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
				transformed, err := transform.TransformTransaction(tx, lhe)
				if err != nil {
					cmdLogger.LogError(fmt.Errorf("could not transform transaction in ledger %d: %s", seq, err))
					numFailures++
					continue
				}

				numBytes, err := ExportEntry(transformed, outFile, commonArgs.Extra)
				if err != nil {
					cmdLogger.LogError(fmt.Errorf("could not export transaction in ledger %d: %s", seq, err))
					numFailures++
					continue
				}
				totalNumBytes += numBytes

				if commonArgs.WriteParquet {
					transformedTransaction = append(transformedTransaction, transformed)
				}
			}
			txReader.Close()
		})

		if commonArgs.EndNum > 0 {
			cmdLogger.Info("Number of bytes written: ", totalNumBytes)
			PrintTransformStats(attempts, numFailures)
			MaybeUpload(cloudCredentials, cloudStorageBucket, cloudProvider, path)
			if commonArgs.WriteParquet {
				MaybeUpload(cloudCredentials, cloudStorageBucket, cloudProvider, parquetPath)
				WriteParquet(transformedTransaction, parquetPath, new(transform.TransactionOutputParquet))
			}
		}
	},
}

func init() {
	rootCmd.AddCommand(transactionsCmd)
	utils.AddCommonFlags(transactionsCmd.Flags())
	utils.AddArchiveFlags("transactions", transactionsCmd.Flags())
	utils.AddCloudStorageFlags(transactionsCmd.Flags())

	/*
		Current flags:
			start-ledger: the ledger sequence number for the beginning of the export period
			end-ledger: the ledger sequence number for the end of the export range (*required)

			limit: maximum number of transactions to export
				TODO: measure a good default value that ensures all transactions within a 5 minute period will be exported with a single call
				The current max_tx_set_size is 1000 and there are 60 new ledgers in a 5 minute period:
					1000*60 = 60000

			output-file: filename of the output file

		TODO: implement extra flags if possible
			serialize-method: the method for serialization of the output data (JSON, XDR, etc)
			start and end time as a replacement for start and end sequence numbers
	*/
}
