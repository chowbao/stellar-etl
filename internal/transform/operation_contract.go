package transform

import (
	"github.com/stellar/go-stellar-sdk/amount"
	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/support/contractevents"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stellar/stellar-etl/v2/internal/utils"
)

func getTransactionV1Envelope(transactionEnvelope xdr.TransactionEnvelope) xdr.TransactionV1Envelope {
	switch transactionEnvelope.Type {
	case xdr.EnvelopeTypeEnvelopeTypeTx:
		return transactionEnvelope.MustV1()
	case xdr.EnvelopeTypeEnvelopeTypeTxFeeBump:
		return transactionEnvelope.MustFeeBump().Tx.InnerTx.MustV1()
	}

	return xdr.TransactionV1Envelope{}
}

func contractIdFromTxEnvelope(transactionEnvelope xdr.TransactionV1Envelope) string {
	for _, ledgerKey := range transactionEnvelope.Tx.Ext.SorobanData.Resources.Footprint.ReadWrite {
		contractId := contractIdFromContractData(ledgerKey)
		if contractId != "" {
			return contractId
		}
	}

	for _, ledgerKey := range transactionEnvelope.Tx.Ext.SorobanData.Resources.Footprint.ReadOnly {
		contractId := contractIdFromContractData(ledgerKey)
		if contractId != "" {
			return contractId
		}
	}

	return ""
}

func contractIdFromContractData(ledgerKey xdr.LedgerKey) string {
	contractData, ok := ledgerKey.GetContractData()
	if !ok {
		return ""
	}
	contractIdHash, ok := contractData.Contract.GetContractId()
	if !ok {
		return ""
	}

	contractIdByte, _ := contractIdHash.MarshalBinary()
	contractId, _ := strkey.Encode(strkey.VersionByteContract, contractIdByte)
	return contractId
}

func contractCodeHashFromTxEnvelope(transactionEnvelope xdr.TransactionV1Envelope) string {
	for _, ledgerKey := range transactionEnvelope.Tx.Ext.SorobanData.Resources.Footprint.ReadOnly {
		contractCode := contractCodeFromContractData(ledgerKey)
		if contractCode != "" {
			return contractCode
		}
	}

	for _, ledgerKey := range transactionEnvelope.Tx.Ext.SorobanData.Resources.Footprint.ReadWrite {
		contractCode := contractCodeFromContractData(ledgerKey)
		if contractCode != "" {
			return contractCode
		}
	}

	return ""
}

func ledgerKeyHashFromTxEnvelope(transactionEnvelope xdr.TransactionV1Envelope) []string {
	var ledgerKeyHash []string
	for _, ledgerKey := range transactionEnvelope.Tx.Ext.SorobanData.Resources.Footprint.ReadOnly {
		if utils.LedgerKeyToLedgerKeyHash(ledgerKey) != "" {
			ledgerKeyHash = append(ledgerKeyHash, utils.LedgerKeyToLedgerKeyHash(ledgerKey))
		}
	}

	for _, ledgerKey := range transactionEnvelope.Tx.Ext.SorobanData.Resources.Footprint.ReadWrite {
		if utils.LedgerKeyToLedgerKeyHash(ledgerKey) != "" {
			ledgerKeyHash = append(ledgerKeyHash, utils.LedgerKeyToLedgerKeyHash(ledgerKey))
		}
	}

	return ledgerKeyHash
}

func contractCodeFromContractData(ledgerKey xdr.LedgerKey) string {
	contractCode, ok := ledgerKey.GetContractCode()
	if !ok {
		return ""
	}

	contractCodeHash := contractCode.Hash.HexString()
	return contractCodeHash
}

func filterEvents(diagnosticEvents []xdr.DiagnosticEvent) []xdr.ContractEvent {
	var filtered []xdr.ContractEvent
	for _, diagnosticEvent := range diagnosticEvents {
		if !diagnosticEvent.InSuccessfulContractCall || diagnosticEvent.Event.Type != xdr.ContractEventTypeContract {
			continue
		}
		filtered = append(filtered, diagnosticEvent.Event)
	}
	return filtered
}

// Searches an operation for SAC events that are of a type which represent
// asset balances having changed.
//
// SAC events have a one-to-one association to SAC contract fn invocations.
// i.e. invoke the 'mint' function, will trigger one Mint Event to be emitted capturing the fn args.
//
// SAC events that involve asset balance changes follow some standard data formats.
// The 'amount' in the event is expressed as Int128Parts, which carries a sign, however it's expected
// that value will not be signed as it represents a absolute delta, the event type can provide the
// context of whether an amount was considered incremental or decremental, i.e. credit or debit to a balance.
func (operation *transactionOperationWrapper) parseAssetBalanceChangesFromContractEvents() ([]map[string]interface{}, error) {
	balanceChanges := []map[string]interface{}{}

	diagnosticEvents, err := operation.transaction.GetDiagnosticEvents()
	if err != nil {
		// this operation in this context must be an InvokeHostFunctionOp, therefore V3Meta should be present
		// as it's in same soroban model, so if any err, it's real,
		return nil, err
	}

	for _, contractEvent := range filterEvents(diagnosticEvents) {
		// Parse the xdr contract event to contractevents.StellarAssetContractEvent model

		// has some convenience like to/from attributes are expressed in strkey format for accounts(G...) and contracts(C...)
		if sacEvent, err := contractevents.NewStellarAssetContractEvent(&contractEvent, operation.network); err == nil {
			switch sacEvent.GetType() {
			case contractevents.EventTypeTransfer:
				transferEvt := sacEvent.(*contractevents.TransferEvent)
				balanceChanges = append(balanceChanges, createSACBalanceChangeEntry(transferEvt.From, transferEvt.To, transferEvt.Amount, transferEvt.Asset, "transfer"))
			case contractevents.EventTypeMint:
				mintEvt := sacEvent.(*contractevents.MintEvent)
				balanceChanges = append(balanceChanges, createSACBalanceChangeEntry("", mintEvt.To, mintEvt.Amount, mintEvt.Asset, "mint"))
			case contractevents.EventTypeClawback:
				clawbackEvt := sacEvent.(*contractevents.ClawbackEvent)
				balanceChanges = append(balanceChanges, createSACBalanceChangeEntry(clawbackEvt.From, "", clawbackEvt.Amount, clawbackEvt.Asset, "clawback"))
			case contractevents.EventTypeBurn:
				burnEvt := sacEvent.(*contractevents.BurnEvent)
				balanceChanges = append(balanceChanges, createSACBalanceChangeEntry(burnEvt.From, "", burnEvt.Amount, burnEvt.Asset, "burn"))
			}
		}
	}

	return balanceChanges, nil
}

func parseAssetBalanceChangesFromContractEvents(transaction ingest.LedgerTransaction, network string) ([]map[string]interface{}, error) {
	balanceChanges := []map[string]interface{}{}

	diagnosticEvents, err := transaction.GetDiagnosticEvents()
	if err != nil {
		// this operation in this context must be an InvokeHostFunctionOp, therefore V3Meta should be present
		// as it's in same soroban model, so if any err, it's real,
		return nil, err
	}

	for _, contractEvent := range filterEvents(diagnosticEvents) {
		// Parse the xdr contract event to contractevents.StellarAssetContractEvent model

		// has some convenience like to/from attributes are expressed in strkey format for accounts(G...) and contracts(C...)
		if sacEvent, err := contractevents.NewStellarAssetContractEvent(&contractEvent, network); err == nil {
			switch sacEvent.GetType() {
			case contractevents.EventTypeTransfer:
				transferEvt := sacEvent.(*contractevents.TransferEvent)
				balanceChanges = append(balanceChanges, createSACBalanceChangeEntry(transferEvt.From, transferEvt.To, transferEvt.Amount, transferEvt.Asset, "transfer"))
			case contractevents.EventTypeMint:
				mintEvt := sacEvent.(*contractevents.MintEvent)
				balanceChanges = append(balanceChanges, createSACBalanceChangeEntry("", mintEvt.To, mintEvt.Amount, mintEvt.Asset, "mint"))
			case contractevents.EventTypeClawback:
				clawbackEvt := sacEvent.(*contractevents.ClawbackEvent)
				balanceChanges = append(balanceChanges, createSACBalanceChangeEntry(clawbackEvt.From, "", clawbackEvt.Amount, clawbackEvt.Asset, "clawback"))
			case contractevents.EventTypeBurn:
				burnEvt := sacEvent.(*contractevents.BurnEvent)
				balanceChanges = append(balanceChanges, createSACBalanceChangeEntry(burnEvt.From, "", burnEvt.Amount, burnEvt.Asset, "burn"))
			}
		}
	}

	return balanceChanges, nil
}

// fromAccount   - strkey format of contract or address
// toAccount     - strkey format of contract or address, or nillable
// amountChanged - absolute value that asset balance changed
// asset         - the fully qualified issuer:code for asset that had balance change
// changeType    - the type of source sac event that triggered this change
//
// return        - a balance changed record expressed as map of key/value's
func createSACBalanceChangeEntry(fromAccount string, toAccount string, amountChanged xdr.Int128Parts, asset xdr.Asset, changeType string) map[string]interface{} {
	balanceChange := map[string]interface{}{}

	if fromAccount != "" {
		balanceChange["from"] = fromAccount
	}
	if toAccount != "" {
		balanceChange["to"] = toAccount
	}

	balanceChange["type"] = changeType
	balanceChange["amount"] = amount.String128(amountChanged)
	addAssetDetails(balanceChange, asset, "")
	return balanceChange
}

