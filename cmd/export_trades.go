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
		startNum, path, parquetPath, _ := utils.MustArchiveFlags(cmd.Flags(), cmdLogger)
		env := utils.GetEnvironmentDetails(commonArgs)
		cloudStorageBucket, cloudCredentials, cloudProvider := utils.MustCloudStorageFlags(cmd.Flags(), cmdLogger)

		numFailures := 0
		totalNumBytes := 0
		attempts := 0
		var transformedTrades []transform.SchemaParquet

		StreamLedgers(startNum, commonArgs.EndNum, path, commonArgs.UseCaptiveCore, env, func(seq uint32, lcm xdr.LedgerCloseMeta, outFile *os.File) {
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
						attempts++
						tradeInput := input.TradeTransformInput{
							OperationIndex:     int32(index),
							Transaction:        tx,
							CloseTime:          closeTime,
							OperationHistoryID: toid.New(int32(seq), int32(tx.Index), int32(index)).ToInt64(),
						}
						trades, err := transform.TransformTrade(tradeInput.OperationIndex, tradeInput.OperationHistoryID, tradeInput.Transaction, tradeInput.CloseTime)
						if err != nil {
							cmdLogger.LogError(fmt.Errorf("could not transform trade in ledger %d: %s", seq, err))
							numFailures++
							continue
						}

						for _, t := range trades {
							numBytes, err := ExportEntry(t, outFile, commonArgs.Extra)
							if err != nil {
								cmdLogger.LogError(fmt.Errorf("could not export trade in ledger %d: %s", seq, err))
								numFailures++
								continue
							}
							totalNumBytes += numBytes

							if commonArgs.WriteParquet {
								transformedTrades = append(transformedTrades, t)
							}
						}
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
				MaybeUpload(cloudCredentials, cloudStorageBucket, cloudProvider, parquetPath)
				WriteParquet(transformedTrades, parquetPath, new(transform.TradeOutputParquet))
			}
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
