package utils

import (
	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/txnbuild"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// CreateSampleTx creates a transaction with a single operation (BumpSequence), the min base fee, and infinite timebounds
func CreateSampleTx(sequence int64, operationCount int) xdr.TransactionEnvelope {
	kp, err := keypair.Random()
	PanicOnError(err)

	operations := []txnbuild.Operation{}
	operationType := &txnbuild.BumpSequence{
		BumpTo: 0,
	}
	for i := 0; i < operationCount; i++ {
		operations = append(operations, operationType)
	}

	sourceAccount := txnbuild.NewSimpleAccount(kp.Address(), int64(0))
	tx, err := txnbuild.NewTransaction(
		txnbuild.TransactionParams{
			SourceAccount: &sourceAccount,
			Operations:    operations,
			BaseFee:       txnbuild.MinBaseFee,
			Preconditions: txnbuild.Preconditions{TimeBounds: txnbuild.NewInfiniteTimeout()},
		},
	)
	PanicOnError(err)

	env := tx.ToXDR()
	return env
}

// CreateSampleResultMeta creates Transaction results with the desired success flag and number of sub operation results
func CreateSampleResultMeta(successful bool, subOperationCount int) xdr.TransactionResultMeta {
	resultCode := xdr.TransactionResultCodeTxFailed
	if successful {
		resultCode = xdr.TransactionResultCodeTxSuccess
	}
	operationResults := []xdr.OperationResult{}
	operationResultTr := &xdr.OperationResultTr{
		Type: xdr.OperationTypeCreateAccount,
		CreateAccountResult: &xdr.CreateAccountResult{
			Code: 0,
		},
	}

	for i := 0; i < subOperationCount; i++ {
		operationResults = append(operationResults, xdr.OperationResult{
			Code: xdr.OperationResultCodeOpInner,
			Tr:   operationResultTr,
		})
	}

	return xdr.TransactionResultMeta{
		Result: xdr.TransactionResultPair{
			Result: xdr.TransactionResult{
				Result: xdr.TransactionResultResult{
					Code:    resultCode,
					Results: &operationResults,
				},
			},
		},
	}
}

func CreateSampleResultPair(successful bool, subOperationCount int) xdr.TransactionResultPair {
	resultCode := xdr.TransactionResultCodeTxFailed
	if successful {
		resultCode = xdr.TransactionResultCodeTxSuccess
	}
	operationResults := []xdr.OperationResult{}
	operationResultTr := &xdr.OperationResultTr{
		Type: xdr.OperationTypeCreateAccount,
		CreateAccountResult: &xdr.CreateAccountResult{
			Code: 0,
		},
	}

	for i := 0; i < subOperationCount; i++ {
		operationResults = append(operationResults, xdr.OperationResult{
			Code: xdr.OperationResultCodeOpInner,
			Tr:   operationResultTr,
		})
	}

	return xdr.TransactionResultPair{
		Result: xdr.TransactionResult{
			Result: xdr.TransactionResultResult{
				Code:    resultCode,
				Results: &operationResults,
			},
		},
	}
}

func CreateSampleTxMeta(subOperationCount int, AssetA, AssetB xdr.Asset) *xdr.TransactionMetaV1 {
	operationMeta := []xdr.OperationMeta{}
	for i := 0; i < subOperationCount; i++ {
		operationMeta = append(operationMeta, xdr.OperationMeta{
			Changes: xdr.LedgerEntryChanges{},
		})
	}

	operationMeta = AddLPOperations(operationMeta, AssetA, AssetB)
	operationMeta = AddLPOperations(operationMeta, AssetA, AssetB)

	operationMeta = append(operationMeta, xdr.OperationMeta{
		Changes: xdr.LedgerEntryChanges{},
	})

	return &xdr.TransactionMetaV1{
		Operations: operationMeta,
	}
}

func AddLPOperations(txMeta []xdr.OperationMeta, AssetA, AssetB xdr.Asset) []xdr.OperationMeta {
	txMeta = append(txMeta, xdr.OperationMeta{
		Changes: xdr.LedgerEntryChanges{
			xdr.LedgerEntryChange{
				Type: xdr.LedgerEntryChangeTypeLedgerEntryState,
				State: &xdr.LedgerEntry{
					Data: xdr.LedgerEntryData{
						Type: xdr.LedgerEntryTypeLiquidityPool,
						LiquidityPool: &xdr.LiquidityPoolEntry{
							LiquidityPoolId: xdr.PoolId{1, 2, 3, 4, 5, 6, 7, 8, 9},
							Body: xdr.LiquidityPoolEntryBody{
								Type: xdr.LiquidityPoolTypeLiquidityPoolConstantProduct,
								ConstantProduct: &xdr.LiquidityPoolEntryConstantProduct{
									Params: xdr.LiquidityPoolConstantProductParameters{
										AssetA: AssetA,
										AssetB: AssetB,
										Fee:    30,
									},
									ReserveA:                 100000,
									ReserveB:                 1000,
									TotalPoolShares:          500,
									PoolSharesTrustLineCount: 25,
								},
							},
						},
					},
				},
			},
			xdr.LedgerEntryChange{
				Type: xdr.LedgerEntryChangeTypeLedgerEntryUpdated,
				Updated: &xdr.LedgerEntry{
					Data: xdr.LedgerEntryData{
						Type: xdr.LedgerEntryTypeLiquidityPool,
						LiquidityPool: &xdr.LiquidityPoolEntry{
							LiquidityPoolId: xdr.PoolId{1, 2, 3, 4, 5, 6, 7, 8, 9},
							Body: xdr.LiquidityPoolEntryBody{
								Type: xdr.LiquidityPoolTypeLiquidityPoolConstantProduct,
								ConstantProduct: &xdr.LiquidityPoolEntryConstantProduct{
									Params: xdr.LiquidityPoolConstantProductParameters{
										AssetA: AssetA,
										AssetB: AssetB,
										Fee:    30,
									},
									ReserveA:                 101000,
									ReserveB:                 1100,
									TotalPoolShares:          502,
									PoolSharesTrustLineCount: 26,
								},
							},
						},
					},
				},
			},
		}})

	return txMeta
}
