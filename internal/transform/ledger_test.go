package transform

import (
	"fmt"
	"testing"
	"time"

	"github.com/stellar/stellar-etl/internal/utils"

	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/assert"
)

func TestTransformLedger(t *testing.T) {
	type transformTest struct {
		input      xdr.LedgerCloseMeta
		wantOutput LedgerOutput
		wantErr    error
	}
	hardCodedLedger, err := makeLedgerTestInput()
	assert.NoError(t, err)

	hardCodedOutput, err := makeLedgerTestOutput()
	assert.NoError(t, err)

	tests := []transformTest{
		{
			wrapLedgerHeader(xdr.LedgerHeader{
				TotalCoins: -1,
			}),
			LedgerOutput{},
			fmt.Errorf("The total number of coins (-1) is negative for ledger 0 (ledger id=0)"),
		},
		{
			wrapLedgerHeader(xdr.LedgerHeader{
				FeePool: -1,
			}),
			LedgerOutput{},
			fmt.Errorf("The fee pool (-1) is negative for ledger 0 (ledger id=0)"),
		},
		{
			wrapLedgerHeaderWithTransactions(xdr.LedgerHeader{
				MaxTxSetSize: 0,
			}, 2),
			LedgerOutput{},
			fmt.Errorf("for ledger 0 (ledger id=0): The number of transactions and results are different (2 != 0)"),
		},
		{
			hardCodedLedger,
			hardCodedOutput,
			nil,
		},
	}

	for _, test := range tests {
		actualOutput, actualError := TransformLedger(test.input)
		assert.Equal(t, test.wantErr, actualError)
		assert.Equal(t, test.wantOutput, actualOutput)
	}
}

func makeLedgerTestOutput() (output LedgerOutput, err error) {
	correctTime, err := time.Parse("2006-1-2 15:04:05 MST", "2020-07-12 20:09:07 UTC")
	if err != nil {
		return
	}

	correctBytes := []byte{0x41, 0x41, 0x41, 0x41, 0x44, 0x66, 0x59, 0x38, 0x46, 0x64, 0x44, 0x71, 0x39, 0x49, 0x72, 0x37, 0x31, 0x31, 0x47, 0x6b, 0x78, 0x4e, 0x2b, 0x74, 0x35, 0x55, 0x6f, 0x30, 0x53, 0x41, 0x55, 0x38, 0x52, 0x38, 0x57, 0x6e, 0x48, 0x57, 0x49, 0x6d, 0x61, 0x4b, 0x34, 0x4d, 0x77, 0x71, 0x49, 0x49, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x58, 0x77, 0x74, 0x74, 0x34, 0x77, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x48, 0x53, 0x6d, 0x53, 0x55, 0x4f, 0x6f, 0x68, 0x36, 0x7a, 0x37, 0x48, 0x6c, 0x62, 0x59, 0x51, 0x41, 0x41, 0x45, 0x49, 0x4c, 0x41, 0x79, 0x55, 0x61, 0x4a, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x5a, 0x41, 0x42, 0x4d, 0x53, 0x30, 0x41, 0x41, 0x41, 0x41, 0x50, 0x6f, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41}
	output = LedgerOutput{
		Sequence:           uint32(30578981),
		LedgerID:           131335723340005376,
		LedgerHash:         "26932dc4d84b5fabe9ae744cb43ce4c6daccf98c86a991b2a14945b1adac4d59",
		PreviousLedgerHash: "f63c15d0eaf48afbd751a4c4dfade54a3448053c47c5a71d622668ae0cc2a208",
		LedgerHeader:       string(correctBytes),
		ClosedAt:           correctTime,

		TotalCoins:      1054439020873472865,
		FeePool:         18153766209161,
		BaseFee:         100,
		BaseReserve:     5000000,
		MaxTxSetSize:    1000,
		ProtocolVersion: 13,

		TransactionCount:           1,
		OperationCount:             10,
		SuccessfulTransactionCount: 1,
		FailedTransactionCount:     1,
		TxSetOperationCount:        "13",
	}
	return
}

func makeLedgerTestInput() (lcm xdr.LedgerCloseMeta, err error) {
	hardCodedTxSet := xdr.TransactionSet{
		Txs: []xdr.TransactionEnvelope{
			utils.CreateSampleTx(0),
			utils.CreateSampleTx(1),
		},
	}
	hardCodedTxProcessing := []xdr.TransactionResultMeta{
		utils.CreateSampleResultMeta(false, 3),
		utils.CreateSampleResultMeta(true, 10),
	}
	lcm, err = xdr.NewLedgerCloseMeta(0, xdr.LedgerCloseMetaV0{
		LedgerHeader: xdr.LedgerHeaderHistoryEntry{
			Header: xdr.LedgerHeader{
				LedgerSeq:          30578981,
				TotalCoins:         1054439020873472865,
				FeePool:            18153766209161,
				BaseFee:            100,
				BaseReserve:        5000000,
				MaxTxSetSize:       1000,
				LedgerVersion:      13,
				PreviousLedgerHash: xdr.Hash{0xf6, 0x3c, 0x15, 0xd0, 0xea, 0xf4, 0x8a, 0xfb, 0xd7, 0x51, 0xa4, 0xc4, 0xdf, 0xad, 0xe5, 0x4a, 0x34, 0x48, 0x5, 0x3c, 0x47, 0xc5, 0xa7, 0x1d, 0x62, 0x26, 0x68, 0xae, 0xc, 0xc2, 0xa2, 0x8},
				ScpValue:           xdr.StellarValue{CloseTime: 1594584547},
			},
			Hash: xdr.Hash{0x26, 0x93, 0x2d, 0xc4, 0xd8, 0x4b, 0x5f, 0xab, 0xe9, 0xae, 0x74, 0x4c, 0xb4, 0x3c, 0xe4, 0xc6, 0xda, 0xcc, 0xf9, 0x8c, 0x86, 0xa9, 0x91, 0xb2, 0xa1, 0x49, 0x45, 0xb1, 0xad, 0xac, 0x4d, 0x59},
		},
		TxSet:        hardCodedTxSet,
		TxProcessing: hardCodedTxProcessing,
	})
	return
}
func wrapLedgerHeaderWithTransactions(header xdr.LedgerHeader, numTransactions int) xdr.LedgerCloseMeta {
	transactionEnvelopes := []xdr.TransactionEnvelope{}
	for txNum := 0; txNum < numTransactions; txNum++ {
		transactionEnvelopes = append(transactionEnvelopes, utils.CreateSampleTx(int64(txNum)))
	}
	lcm, _ := xdr.NewLedgerCloseMeta(0, xdr.LedgerCloseMetaV0{
		LedgerHeader: xdr.LedgerHeaderHistoryEntry{
			Header: header,
		},
		TxSet: xdr.TransactionSet{Txs: transactionEnvelopes},
	})
	return lcm
}

func wrapLedgerHeader(header xdr.LedgerHeader) xdr.LedgerCloseMeta {
	return wrapLedgerHeaderWithTransactions(header, 0)
}
