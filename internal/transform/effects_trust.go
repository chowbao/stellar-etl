package transform

import (
	"sort"

	"github.com/stellar/go-stellar-sdk/amount"
	"github.com/stellar/go-stellar-sdk/protocols/horizon/base"
	"github.com/stellar/go-stellar-sdk/xdr"
)

func (e *effectsWrapper) addChangeTrustEffects() error {
	source := e.operation.SourceAccount()

	op := e.operation.operation.Body.MustChangeTrustOp()
	changes, err := e.operation.transaction.GetOperationChanges(e.operation.index)
	if err != nil {
		return err
	}

	// NOTE:  when an account trusts itself, the transaction is successful but
	// no ledger entries are actually modified.
	for _, change := range changes {
		if change.Type != xdr.LedgerEntryTypeTrustline {
			continue
		}

		var (
			effect    EffectType
			trustLine xdr.TrustLineEntry
		)

		switch {
		case change.Pre == nil && change.Post != nil:
			effect = EffectTrustlineCreated
			trustLine = *change.Post.Data.TrustLine
		case change.Pre != nil && change.Post == nil:
			effect = EffectTrustlineRemoved
			trustLine = *change.Pre.Data.TrustLine
		case change.Pre != nil && change.Post != nil:
			effect = EffectTrustlineUpdated
			trustLine = *change.Post.Data.TrustLine
		default:
			panic("Invalid change")
		}

		// We want to add a single effect for change_trust op. If it's modifying
		// credit_asset search for credit_asset trustline, otherwise search for
		// liquidity_pool.
		if op.Line.Type != trustLine.Asset.Type {
			continue
		}

		details := map[string]interface{}{"limit": amount.String(op.Limit)}
		if trustLine.Asset.Type == xdr.AssetTypeAssetTypePoolShare {
			// The only change_trust ops that can modify LP are those with
			// asset=liquidity_pool so *op.Line.LiquidityPool below is available.
			if err := addLiquidityPoolAssetDetails(details, *op.Line.LiquidityPool); err != nil {
				return err
			}
		} else {
			addAssetDetails(details, op.Line.ToAsset(), "")
		}

		e.addMuxed(source, effect, details)
		break
	}

	return nil
}

func (e *effectsWrapper) addAllowTrustEffects() error {
	source := e.operation.SourceAccount()
	op := e.operation.operation.Body.MustAllowTrustOp()
	asset := op.Asset.ToAsset(source.ToAccountId())
	details := map[string]interface{}{
		"trustor": op.Trustor.Address(),
	}
	addAssetDetails(details, asset, "")

	switch {
	case xdr.TrustLineFlags(op.Authorize).IsAuthorized():
		e.addMuxed(source, EffectTrustlineFlagsUpdated, details)
		// Forward compatibility
		setFlags := xdr.Uint32(xdr.TrustLineFlagsAuthorizedFlag)
		e.addTrustLineFlagsEffect(source, &op.Trustor, asset, &setFlags, nil)
	case xdr.TrustLineFlags(op.Authorize).IsAuthorizedToMaintainLiabilitiesFlag():
		e.addMuxed(
			source,
			EffectTrustlineFlagsUpdated,
			details,
		)
		// Forward compatibility
		setFlags := xdr.Uint32(xdr.TrustLineFlagsAuthorizedToMaintainLiabilitiesFlag)
		e.addTrustLineFlagsEffect(source, &op.Trustor, asset, &setFlags, nil)
	default:
		e.addMuxed(source, EffectTrustlineFlagsUpdated, details)
		// Forward compatibility, show both as cleared
		clearFlags := xdr.Uint32(xdr.TrustLineFlagsAuthorizedFlag | xdr.TrustLineFlagsAuthorizedToMaintainLiabilitiesFlag)
		e.addTrustLineFlagsEffect(source, &op.Trustor, asset, nil, &clearFlags)
	}
	return e.addLiquidityPoolRevokedEffect()
}

func (e *effectsWrapper) addSetTrustLineFlagsEffects() error {
	source := e.operation.SourceAccount()
	op := e.operation.operation.Body.MustSetTrustLineFlagsOp()
	e.addTrustLineFlagsEffect(source, &op.Trustor, op.Asset, &op.SetFlags, &op.ClearFlags)
	return e.addLiquidityPoolRevokedEffect()
}

func (e *effectsWrapper) addTrustLineFlagsEffect(
	account *xdr.MuxedAccount,
	trustor *xdr.AccountId,
	asset xdr.Asset,
	setFlags *xdr.Uint32,
	clearFlags *xdr.Uint32) {
	details := map[string]interface{}{
		"trustor": trustor.Address(),
	}
	addAssetDetails(details, asset, "")

	var flagDetailsAdded bool
	if setFlags != nil {
		setTrustLineFlagDetails(details, xdr.TrustLineFlags(*setFlags), true)
		flagDetailsAdded = true
	}
	if clearFlags != nil {
		setTrustLineFlagDetails(details, xdr.TrustLineFlags(*clearFlags), false)
		flagDetailsAdded = true
	}

	if flagDetailsAdded {
		e.addMuxed(account, EffectTrustlineFlagsUpdated, details)
	}
}

func setTrustLineFlagDetails(flagDetails map[string]interface{}, flags xdr.TrustLineFlags, setValue bool) {
	if flags.IsAuthorized() {
		flagDetails["authorized_flag"] = setValue
	}
	if flags.IsAuthorizedToMaintainLiabilitiesFlag() {
		flagDetails["authorized_to_maintain_liabilites"] = setValue
	}
	if flags.IsClawbackEnabledFlag() {
		flagDetails["clawback_enabled_flag"] = setValue
	}
}

func setAuthFlagDetails(flagDetails map[string]interface{}, flags xdr.AccountFlags, setValue bool) {
	if flags.IsAuthRequired() {
		flagDetails["auth_required_flag"] = setValue
	}
	if flags.IsAuthRevocable() {
		flagDetails["auth_revocable_flag"] = setValue
	}
	if flags.IsAuthImmutable() {
		flagDetails["auth_immutable_flag"] = setValue
	}
	if flags.IsAuthClawbackEnabled() {
		flagDetails["auth_clawback_enabled_flag"] = setValue
	}
}

type sortableClaimableBalanceEntries []*xdr.ClaimableBalanceEntry

func (s sortableClaimableBalanceEntries) Len() int           { return len(s) }
func (s sortableClaimableBalanceEntries) Less(i, j int) bool { return s[i].Asset.LessThan(s[j].Asset) }
func (s sortableClaimableBalanceEntries) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

func (e *effectsWrapper) addLiquidityPoolRevokedEffect() error {
	source := e.operation.SourceAccount()
	lp, delta, err := e.operation.getLiquidityPoolAndProductDelta(nil)
	if err != nil {
		if err == errLiquidityPoolChangeNotFound {
			// no revocation happened
			return nil
		}
		return err
	}
	changes, err := e.operation.transaction.GetOperationChanges(e.operation.index)
	if err != nil {
		return err
	}
	assetToCBID := map[string]string{}
	var cbs sortableClaimableBalanceEntries
	for _, change := range changes {
		if change.Type == xdr.LedgerEntryTypeClaimableBalance && change.Pre == nil && change.Post != nil {
			cb := change.Post.Data.ClaimableBalance
			id, err := xdr.MarshalHex(cb.BalanceId)
			if err != nil {
				return err
			}
			assetToCBID[cb.Asset.StringCanonical()] = id
			cbs = append(cbs, cb)
		}
	}
	if len(assetToCBID) == 0 {
		// no claimable balances were created, and thus, no revocation happened
		return nil
	}
	// Core's claimable balance metadata isn't ordered, so we order it ourselves
	// so that effects are ordered consistently
	sort.Sort(cbs)
	for _, cb := range cbs {
		if err := e.addClaimableBalanceEntryCreatedEffects(source, cb); err != nil {
			return err
		}
	}

	reservesRevoked := make([]map[string]string, 0, 2)
	for _, aa := range []base.AssetAmount{
		{
			Asset:  lp.Body.ConstantProduct.Params.AssetA.StringCanonical(),
			Amount: amount.String(-delta.ReserveA),
		},
		{
			Asset:  lp.Body.ConstantProduct.Params.AssetB.StringCanonical(),
			Amount: amount.String(-delta.ReserveB),
		},
	} {
		if cbID, ok := assetToCBID[aa.Asset]; ok {
			assetAmountDetail := map[string]string{
				"asset":                aa.Asset,
				"amount":               aa.Amount,
				"claimable_balance_id": cbID,
			}
			reservesRevoked = append(reservesRevoked, assetAmountDetail)
		}
	}
	details := map[string]interface{}{
		"liquidity_pool":   liquidityPoolDetails(lp),
		"reserves_revoked": reservesRevoked,
		"shares_revoked":   amount.String(-delta.TotalPoolShares),
	}
	e.addMuxed(source, EffectLiquidityPoolRevoked, details)
	return nil
}
