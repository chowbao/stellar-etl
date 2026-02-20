package transform

import (
	"github.com/stellar/go-stellar-sdk/amount"
)

func (e *effectsWrapper) addPaymentEffects() {
	op := e.operation.operation.Body.MustPaymentOp()

	details := map[string]interface{}{"amount": amount.String(op.Amount)}
	addAssetDetails(details, op.Asset, "")

	e.addMuxed(
		&op.Destination,
		EffectAccountCredited,
		details,
	)
	e.addMuxed(
		e.operation.SourceAccount(),
		EffectAccountDebited,
		details,
	)
}

func (e *effectsWrapper) pathPaymentStrictReceiveEffects() error {
	op := e.operation.operation.Body.MustPathPaymentStrictReceiveOp()
	resultSuccess := e.operation.OperationResult().MustPathPaymentStrictReceiveResult().MustSuccess()
	source := e.operation.SourceAccount()

	details := map[string]interface{}{"amount": amount.String(op.DestAmount)}
	addAssetDetails(details, op.DestAsset, "")

	e.addMuxed(
		&op.Destination,
		EffectAccountCredited,
		details,
	)

	result := e.operation.OperationResult().MustPathPaymentStrictReceiveResult()
	details = map[string]interface{}{"amount": amount.String(result.SendAmount())}
	addAssetDetails(details, op.SendAsset, "")

	e.addMuxed(
		source,
		EffectAccountDebited,
		details,
	)

	return e.addIngestTradeEffects(*source, resultSuccess.Offers, false)
}

func (e *effectsWrapper) addPathPaymentStrictSendEffects() error {
	source := e.operation.SourceAccount()
	op := e.operation.operation.Body.MustPathPaymentStrictSendOp()
	resultSuccess := e.operation.OperationResult().MustPathPaymentStrictSendResult().MustSuccess()
	result := e.operation.OperationResult().MustPathPaymentStrictSendResult()

	details := map[string]interface{}{"amount": amount.String(result.DestAmount())}
	addAssetDetails(details, op.DestAsset, "")
	e.addMuxed(&op.Destination, EffectAccountCredited, details)

	details = map[string]interface{}{"amount": amount.String(op.SendAmount)}
	addAssetDetails(details, op.SendAsset, "")
	e.addMuxed(source, EffectAccountDebited, details)

	return e.addIngestTradeEffects(*source, resultSuccess.Offers, true)
}
