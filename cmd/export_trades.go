package cmd

import (
	"fmt"
	"io"
	"os"

	"github.com/sirupsen/logrus"
	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stellar/stellar-etl/v2/internal/toid"

	"github.com/spf13/cobra"
	"github.com/stellar/stellar-etl/v2/internal/input"
	"github.com/stellar/stellar-etl/v2/internal/transform"
	"github.com/stellar/stellar-etl/v2/internal/utils"
)

// tradesCmd represents the trades command
var tradesCmd = &cobra.Command{
	Use:   "export_trades",
	Short: "Exports the trade data",
	Long:  `Exports trade data within the specified range to an output file`,
	Run: func(cmd *cobra.Command, args []string) {
		cmdLogger.SetLevel(logrus.InfoLevel)
		commonArgs := utils.MustCommonFlags(cmd.Flags(), cmdLogger)
		cmdLogger.StrictExport = commonArgs.StrictExport
		startNum, path, parquetPath, limit := utils.MustArchiveFlags(cmd.Flags(), cmdLogger)
		env := utils.GetEnvironmentDetails(commonArgs)
		cloudStorageBucket, cloudCredentials, cloudProvider := utils.MustCloudStorageFlags(cmd.Flags(), cmdLogger)

		if commonArgs.EndNum == 0 {
			StreamUnboundedLedgers(startNum, path, commonArgs.UseCaptiveCore, env, func(seq uint32, lcm xdr.LedgerCloseMeta, outFile *os.File) {
				txReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(env.NetworkPassphrase, lcm)
				if err != nil {
					cmdLogger.LogError(fmt.Errorf("could not create transaction reader for ledger %d: %s", seq, err))
					return
				}

				closeTime, _ := utils.TimePointToUTCTimeStamp(txReader.GetHeader().Header.ScpValue.CloseTime)
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
						if input.OperationResultsInTrade(op) && tx.Result.Successful() {
							tradeInput := input.TradeTransformInput{
								OperationIndex:     int32(index),
								Transaction:        tx,
								CloseTime:          closeTime,
								OperationHistoryID: toid.New(int32(seq), int32(tx.Index), int32(index)).ToInt64(),
							}
							trades, err := transform.TransformTrade(tradeInput.OperationIndex, tradeInput.OperationHistoryID, tradeInput.Transaction, tradeInput.CloseTime)
							if err != nil {
								cmdLogger.LogError(fmt.Errorf("could not transform trade in ledger %d: %s", seq, err))
								continue
							}

							for _, t := range trades {
								_, err = ExportEntry(t, outFile, commonArgs.Extra)
								if err != nil {
									cmdLogger.LogError(fmt.Errorf("could not export trade in ledger %d: %s", seq, err))
								}
							}
						}
					}
				}
				txReader.Close()
			})
			return
		}

		trades, err := input.GetTrades(startNum, commonArgs.EndNum, limit, env, commonArgs.UseCaptiveCore)
		if err != nil {
			cmdLogger.Fatal("could not read trades ", err)
		}

		outFile := MustOutFile(path)
		numFailures := 0
		totalNumBytes := 0
		var transformedTrades []transform.SchemaParquet
		for _, tradeInput := range trades {
			trades, err := transform.TransformTrade(tradeInput.OperationIndex, tradeInput.OperationHistoryID, tradeInput.Transaction, tradeInput.CloseTime)
			if err != nil {
				parsedID := toid.Parse(tradeInput.OperationHistoryID)
				cmdLogger.LogError(fmt.Errorf("from ledger %d, transaction %d, operation %d: %v", parsedID.LedgerSequence, parsedID.TransactionOrder, parsedID.OperationOrder, err))
				numFailures += 1
				continue
			}

			for _, transformed := range trades {
				numBytes, err := ExportEntry(transformed, outFile, commonArgs.Extra)
				if err != nil {
					cmdLogger.LogError(err)
					numFailures += 1
					continue
				}
				totalNumBytes += numBytes

				if commonArgs.WriteParquet {
					transformedTrades = append(transformedTrades, transformed)
				}
			}
		}

		outFile.Close()
		cmdLogger.Info("Number of bytes written: ", totalNumBytes)

		PrintTransformStats(len(trades), numFailures)

		MaybeUpload(cloudCredentials, cloudStorageBucket, cloudProvider, path)

		if commonArgs.WriteParquet {
			MaybeUpload(cloudCredentials, cloudStorageBucket, cloudProvider, parquetPath)
			WriteParquet(transformedTrades, parquetPath, new(transform.TradeOutputParquet))
		}
	},
}

func init() {
	rootCmd.AddCommand(tradesCmd)
	utils.AddCommonFlags(tradesCmd.Flags())
	utils.AddArchiveFlags("trades", tradesCmd.Flags())
	utils.AddCloudStorageFlags(tradesCmd.Flags())
	/*
		TODO: implement extra flags if possible
			serialize-method: the method for serialization of the output data (JSON, XDR, etc)
			start and end time as a replacement for start and end sequence numbers
	*/
}
