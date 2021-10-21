package cmd

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/stellar/stellar-etl/internal/input"
	"github.com/stellar/stellar-etl/internal/transform"
	"github.com/stellar/stellar-etl/internal/utils"

	"github.com/stellar/go/xdr"
)

var poolsCmd = &cobra.Command{
	Use:   "export_pools",
	Short: "Exports the liquidity pools data.",
	Long: `Exports historical liquidity pools data from the genesis ledger to the provided end-ledger to an output file. 
The command reads from the bucket list, which includes the full history of the Stellar ledger. As a result, it 
should be used in an initial data dump. In order to get liqudity pools information within a specified ledger range, see 
the export_ledger_entry_changes command.`,
	Run: func(cmd *cobra.Command, args []string) {
		cmdLogger.SetLevel(logrus.InfoLevel)
		endNum, useStdout, strictExport, isTest := utils.MustCommonFlags(cmd.Flags(), cmdLogger)
		env := utils.GetEnvironmentDetails(isTest)
		path := utils.MustBucketFlags(cmd.Flags(), cmdLogger)

		pools, err := input.GetEntriesFromGenesis(endNum, xdr.LedgerEntryTypeLiquidityPool, env.ArchiveURLs)
		if err != nil {
			cmdLogger.Fatal("could not read accounts: ", err)
		}

		var outFile *os.File
		if !useStdout {
			outFile = mustOutFile(path)
		}

		failures := 0
		numBytes := 0
		for _, pool := range pools {
			transformed, err := transform.TransformPool(pool)
			if err != nil {
				if strictExport {
					cmdLogger.Fatal("could not transform pool", err)
				} else {
					cmdLogger.Warning("could not transform pool", err)
					failures++
					continue
				}
			}

			marshalled, err := json.Marshal(transformed)
			if err != nil {
				if strictExport {
					cmdLogger.Fatal("could not json encode account", err)
				} else {
					cmdLogger.Warning("could not json encode account", err)
					failures++
					continue
				}
			}

			if !useStdout {
				nb, err := outFile.Write(marshalled)
				if err != nil {
					cmdLogger.Info("Error writing accounts to file: ", err)
				}
				numBytes += nb
				outFile.WriteString("\n")
			} else {
				fmt.Println(string(marshalled))
			}

		}

		if !strictExport {
			printLog := true
			if !useStdout {
				outFile.Close()
				printLog = false
				cmdLogger.Info("Number of bytes written: ", numBytes)
			}
			printTransformStats(len(pools), failures, printLog)
		}
	},
}

func init() {
	rootCmd.AddCommand(poolsCmd)
	utils.AddCommonFlags(poolsCmd.Flags())
	utils.AddBucketFlags("pools", poolsCmd.Flags())
	accountsCmd.MarkFlagRequired("end-ledger")
	/*
		Current flags:
			end-ledger: the ledger sequence number for the end of the export range (required)
			output-file: filename of the output file
			stdout: if set, output is printed to stdout

		TODO: implement extra flags if possible
			serialize-method: the method for serialization of the output data (JSON, XDR, etc)
			 end time as a replacement for end sequence numbers
	*/
}
