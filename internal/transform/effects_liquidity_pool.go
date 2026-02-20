package transform

import (
	"strconv"

	"github.com/stellar/go-stellar-sdk/amount"
	"github.com/stellar/go-stellar-sdk/protocols/horizon/base"
	"github.com/stellar/go-stellar-sdk/xdr"
)

func liquidityPoolDetails(lp *xdr.LiquidityPoolEntry) map[string]interface{} {
	return map[string]interface{}{
		"id":               PoolIDToString(lp.LiquidityPoolId),
		"fee_bp":           uint32(lp.Body.ConstantProduct.Params.Fee),
		"type":             "constant_product",
		"total_trustlines": strconv.FormatInt(int64(lp.Body.ConstantProduct.PoolSharesTrustLineCount), 10),
		"total_shares":     amount.String(lp.Body.ConstantProduct.TotalPoolShares),
		"reserves": []base.AssetAmount{
			{
				Asset:  lp.Body.ConstantProduct.Params.AssetA.StringCanonical(),
				Amount: amount.String(lp.Body.ConstantProduct.ReserveA),
			},
			{
				Asset:  lp.Body.ConstantProduct.Params.AssetB.StringCanonical(),
				Amount: amount.String(lp.Body.ConstantProduct.ReserveB),
			},
		},
	}
}

func (e *effectsWrapper) addLiquidityPoolDepositEffect() error {
	op := e.operation.operation.Body.MustLiquidityPoolDepositOp()
	lp, delta, err := e.operation.getLiquidityPoolAndProductDelta(&op.LiquidityPoolId)
	if err != nil {
		return err
	}
	details := map[string]interface{}{
		"liquidity_pool": liquidityPoolDetails(lp),
		"reserves_deposited": []base.AssetAmount{
			{
				Asset:  lp.Body.ConstantProduct.Params.AssetA.StringCanonical(),
				Amount: amount.String(delta.ReserveA),
			},
			{
				Asset:  lp.Body.ConstantProduct.Params.AssetB.StringCanonical(),
				Amount: amount.String(delta.ReserveB),
			},
		},
		"shares_received": amount.String(delta.TotalPoolShares),
	}
	e.addMuxed(e.operation.SourceAccount(), EffectLiquidityPoolDeposited, details)
	return nil
}

func (e *effectsWrapper) addLiquidityPoolWithdrawEffect() error {
	op := e.operation.operation.Body.MustLiquidityPoolWithdrawOp()
	lp, delta, err := e.operation.getLiquidityPoolAndProductDelta(&op.LiquidityPoolId)
	if err != nil {
		return err
	}
	details := map[string]interface{}{
		"liquidity_pool": liquidityPoolDetails(lp),
		"reserves_received": []base.AssetAmount{
			{
				Asset:  lp.Body.ConstantProduct.Params.AssetA.StringCanonical(),
				Amount: amount.String(-delta.ReserveA),
			},
			{
				Asset:  lp.Body.ConstantProduct.Params.AssetB.StringCanonical(),
				Amount: amount.String(-delta.ReserveB),
			},
		},
		"shares_redeemed": amount.String(-delta.TotalPoolShares),
	}
	e.addMuxed(e.operation.SourceAccount(), EffectLiquidityPoolWithdrew, details)
	return nil
}
