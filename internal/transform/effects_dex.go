package transform

import (
	"github.com/stellar/go-stellar-sdk/amount"
	"github.com/stellar/go-stellar-sdk/xdr"
)

func (e *effectsWrapper) addManageSellOfferEffects() error {
	source := e.operation.SourceAccount()
	result := e.operation.OperationResult().MustManageSellOfferResult().MustSuccess()
	return e.addIngestTradeEffects(*source, result.OffersClaimed, false)
}

func (e *effectsWrapper) addManageBuyOfferEffects() error {
	source := e.operation.SourceAccount()
	result := e.operation.OperationResult().MustManageBuyOfferResult().MustSuccess()
	return e.addIngestTradeEffects(*source, result.OffersClaimed, false)
}

func (e *effectsWrapper) addCreatePassiveSellOfferEffect() error {
	result := e.operation.OperationResult()
	source := e.operation.SourceAccount()

	var claims []xdr.ClaimAtom

	// KNOWN ISSUE:  stellar-core creates results for CreatePassiveOffer operations
	// with the wrong result arm set.
	if result.Type == xdr.OperationTypeManageSellOffer {
		claims = result.MustManageSellOfferResult().MustSuccess().OffersClaimed
	} else {
		claims = result.MustCreatePassiveSellOfferResult().MustSuccess().OffersClaimed
	}

	return e.addIngestTradeEffects(*source, claims, false)
}

func (e *effectsWrapper) addIngestTradeEffects(buyer xdr.MuxedAccount, claims []xdr.ClaimAtom, isPathPayment bool) error {
	for _, claim := range claims {
		if claim.AmountSold() == 0 && claim.AmountBought() == 0 {
			continue
		}
		switch claim.Type {
		case xdr.ClaimAtomTypeClaimAtomTypeLiquidityPool:
			if err := e.addClaimLiquidityPoolTradeEffect(claim); err != nil {
				return err
			}
		default:
			e.addClaimTradeEffects(buyer, claim, isPathPayment)
		}
	}
	return nil
}

func (e *effectsWrapper) addClaimTradeEffects(buyer xdr.MuxedAccount, claim xdr.ClaimAtom, isPathPayment bool) {
	seller := claim.SellerId()
	bd, sd := tradeDetails(buyer, seller, claim)

	tradeEffects := []EffectType{
		EffectTrade,
		EffectOfferUpdated,
		EffectOfferRemoved,
		EffectOfferCreated,
	}

	for n, effect := range tradeEffects {
		// skip EffectOfferCreated if OperationType is path_payment
		if n == 3 && isPathPayment {
			continue
		}

		e.addMuxed(
			&buyer,
			effect,
			bd,
		)

		e.addUnmuxed(
			&seller,
			effect,
			sd,
		)
	}
}

func (e *effectsWrapper) addClaimLiquidityPoolTradeEffect(claim xdr.ClaimAtom) error {
	lp, _, err := e.operation.getLiquidityPoolAndProductDelta(&claim.LiquidityPool.LiquidityPoolId)
	if err != nil {
		return err
	}
	details := map[string]interface{}{
		"liquidity_pool": liquidityPoolDetails(lp),
		"sold": map[string]string{
			"asset":  claim.LiquidityPool.AssetSold.StringCanonical(),
			"amount": amount.String(claim.LiquidityPool.AmountSold),
		},
		"bought": map[string]string{
			"asset":  claim.LiquidityPool.AssetBought.StringCanonical(),
			"amount": amount.String(claim.LiquidityPool.AmountBought),
		},
	}
	e.addMuxed(e.operation.SourceAccount(), EffectLiquidityPoolTrade, details)
	return nil
}

func tradeDetails(buyer xdr.MuxedAccount, seller xdr.AccountId, claim xdr.ClaimAtom) (bd map[string]interface{}, sd map[string]interface{}) {
	bd = map[string]interface{}{
		"offer_id":      claim.OfferId(),
		"seller":        seller.Address(),
		"bought_amount": amount.String(claim.AmountSold()),
		"sold_amount":   amount.String(claim.AmountBought()),
	}
	addAssetDetails(bd, claim.AssetSold(), "bought_")
	addAssetDetails(bd, claim.AssetBought(), "sold_")

	sd = map[string]interface{}{
		"offer_id":      claim.OfferId(),
		"bought_amount": amount.String(claim.AmountBought()),
		"sold_amount":   amount.String(claim.AmountSold()),
	}
	addAccountAndMuxedAccountDetails(sd, buyer, "seller")
	addAssetDetails(sd, claim.AssetBought(), "bought_")
	addAssetDetails(sd, claim.AssetSold(), "sold_")

	return
}
