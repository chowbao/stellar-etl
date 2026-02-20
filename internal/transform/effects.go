package transform

import (
	"fmt"
	"sort"

	"github.com/guregu/null"
	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/support/errors"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stellar/stellar-etl/v2/internal/utils"
)

func TransformEffect(transaction ingest.LedgerTransaction, ledgerSeq uint32, ledgerCloseMeta xdr.LedgerCloseMeta, networkPassphrase string) ([]EffectOutput, error) {
	effects := []EffectOutput{}

	outputCloseTime, err := utils.GetCloseTime(ledgerCloseMeta)
	if err != nil {
		return effects, err
	}

	for opi, op := range transaction.Envelope.Operations() {
		operation := transactionOperationWrapper{
			index:          uint32(opi),
			transaction:    transaction,
			operation:      op,
			ledgerSequence: ledgerSeq,
			network:        networkPassphrase,
			ledgerClosed:   outputCloseTime,
		}

		p, err := operation.effects()
		if err != nil {
			return effects, errors.Wrapf(err, "reading operation %v effects", operation.ID())
		}

		effects = append(effects, p...)

	}

	return effects, nil
}

// Effects returns the operation effects
func (operation *transactionOperationWrapper) effects() ([]EffectOutput, error) {
	if !operation.transaction.Result.Successful() {
		return []EffectOutput{}, nil
	}
	var (
		op  = operation.operation
		err error
	)

	changes, err := operation.transaction.GetOperationChanges(operation.index)
	if err != nil {
		return nil, err
	}

	wrapper := &effectsWrapper{
		effects:   []EffectOutput{},
		operation: operation,
	}

	switch operation.OperationType() {
	case xdr.OperationTypeCreateAccount:
		wrapper.addAccountCreatedEffects()
	case xdr.OperationTypePayment:
		wrapper.addPaymentEffects()
	case xdr.OperationTypePathPaymentStrictReceive:
		err = wrapper.pathPaymentStrictReceiveEffects()
	case xdr.OperationTypePathPaymentStrictSend:
		err = wrapper.addPathPaymentStrictSendEffects()
	case xdr.OperationTypeManageSellOffer:
		err = wrapper.addManageSellOfferEffects()
	case xdr.OperationTypeManageBuyOffer:
		err = wrapper.addManageBuyOfferEffects()
	case xdr.OperationTypeCreatePassiveSellOffer:
		err = wrapper.addCreatePassiveSellOfferEffect()
	case xdr.OperationTypeSetOptions:
		wrapper.addSetOptionsEffects()
	case xdr.OperationTypeChangeTrust:
		err = wrapper.addChangeTrustEffects()
	case xdr.OperationTypeAllowTrust:
		err = wrapper.addAllowTrustEffects()
	case xdr.OperationTypeAccountMerge:
		wrapper.addAccountMergeEffects()
	case xdr.OperationTypeInflation:
		wrapper.addInflationEffects()
	case xdr.OperationTypeManageData:
		err = wrapper.addManageDataEffects()
	case xdr.OperationTypeBumpSequence:
		err = wrapper.addBumpSequenceEffects()
	case xdr.OperationTypeCreateClaimableBalance:
		err = wrapper.addCreateClaimableBalanceEffects(changes)
	case xdr.OperationTypeClaimClaimableBalance:
		err = wrapper.addClaimClaimableBalanceEffects(changes)
	case xdr.OperationTypeBeginSponsoringFutureReserves, xdr.OperationTypeEndSponsoringFutureReserves, xdr.OperationTypeRevokeSponsorship:
	// The effects of these operations are obtained  indirectly from the ledger entries
	case xdr.OperationTypeClawback:
		err = wrapper.addClawbackEffects()
	case xdr.OperationTypeClawbackClaimableBalance:
		err = wrapper.addClawbackClaimableBalanceEffects(changes)
	case xdr.OperationTypeSetTrustLineFlags:
		err = wrapper.addSetTrustLineFlagsEffects()
	case xdr.OperationTypeLiquidityPoolDeposit:
		err = wrapper.addLiquidityPoolDepositEffect()
	case xdr.OperationTypeLiquidityPoolWithdraw:
		err = wrapper.addLiquidityPoolWithdrawEffect()
	case xdr.OperationTypeInvokeHostFunction:
		// If there's an invokeHostFunction operation, there's definitely V3
		// meta in the transaction, which means this error is real.
		// TODO: Replace GetContractEvents with TransformContractEvent to get all the events
		contractEvents, innerErr := operation.transaction.GetContractEvents()

		if innerErr != nil {
			return nil, innerErr
		}

		// For now, the only effects are related to the events themselves.
		// Possible add'l work: https://github.com/stellar/go-stellar-sdk/issues/4585
		err = wrapper.addInvokeHostFunctionEffects(contractEvents)
	case xdr.OperationTypeExtendFootprintTtl:
		err = wrapper.addExtendFootprintTtlEffect()
	case xdr.OperationTypeRestoreFootprint:
		err = wrapper.addRestoreFootprintExpirationEffect()
	default:
		return nil, fmt.Errorf("unknown operation type: %s", op.Body.Type)
	}

	if err != nil {
		return nil, err
	}

	// Effects generated for multiple operations. Keep the effect categories
	// separated so they are "together" in case of different order or meta
	// changes generate by core (unordered_map).

	// Sponsorships
	for _, change := range changes {
		if err = wrapper.addLedgerEntrySponsorshipEffects(change); err != nil {
			return nil, err
		}
		wrapper.addSignerSponsorshipEffects(change)
	}

	// Liquidity pools
	for _, change := range changes {
		// Effects caused by ChangeTrust (creation), AllowTrust and SetTrustlineFlags (removal through revocation)
		wrapper.addLedgerEntryLiquidityPoolEffects(change)
	}

	for i := range wrapper.effects {
		wrapper.effects[i].LedgerClosed = operation.ledgerClosed
		wrapper.effects[i].LedgerSequence = operation.ledgerSequence
		wrapper.effects[i].EffectIndex = uint32(i)
		wrapper.effects[i].EffectId = fmt.Sprintf("%d-%d", wrapper.effects[i].OperationID, wrapper.effects[i].EffectIndex)
	}

	return wrapper.effects, nil
}

type effectsWrapper struct {
	effects   []EffectOutput
	operation *transactionOperationWrapper
}

func (e *effectsWrapper) add(address string, addressMuxed null.String, effectType EffectType, details map[string]interface{}) {
	e.effects = append(e.effects, EffectOutput{
		Address:      address,
		AddressMuxed: addressMuxed,
		OperationID:  e.operation.ID(),
		TypeString:   EffectTypeNames[effectType],
		Type:         int32(effectType),
		Details:      details,
	})
}

func (e *effectsWrapper) addUnmuxed(address *xdr.AccountId, effectType EffectType, details map[string]interface{}) {
	e.add(address.Address(), null.String{}, effectType, details)
}

func (e *effectsWrapper) addMuxed(address *xdr.MuxedAccount, effectType EffectType, details map[string]interface{}) {
	var addressMuxed null.String
	if address.Type == xdr.CryptoKeyTypeKeyTypeMuxedEd25519 {
		addressMuxed = null.StringFrom(address.Address())
	}
	accID := address.ToAccountId()
	e.add(accID.Address(), addressMuxed, effectType, details)
}

var sponsoringEffectsTable = map[xdr.LedgerEntryType]struct {
	created, updated, removed EffectType
}{
	xdr.LedgerEntryTypeAccount: {
		created: EffectAccountSponsorshipCreated,
		updated: EffectAccountSponsorshipUpdated,
		removed: EffectAccountSponsorshipRemoved,
	},
	xdr.LedgerEntryTypeTrustline: {
		created: EffectTrustlineSponsorshipCreated,
		updated: EffectTrustlineSponsorshipUpdated,
		removed: EffectTrustlineSponsorshipRemoved,
	},
	xdr.LedgerEntryTypeData: {
		created: EffectDataSponsorshipCreated,
		updated: EffectDataSponsorshipUpdated,
		removed: EffectDataSponsorshipRemoved,
	},
	xdr.LedgerEntryTypeClaimableBalance: {
		created: EffectClaimableBalanceSponsorshipCreated,
		updated: EffectClaimableBalanceSponsorshipUpdated,
		removed: EffectClaimableBalanceSponsorshipRemoved,
	},

	// We intentionally don't have Sponsoring effects for Offer
	// entries because we don't generate creation effects for them.
}

func (e *effectsWrapper) addSignerSponsorshipEffects(change ingest.Change) {
	if change.Type != xdr.LedgerEntryTypeAccount {
		return
	}

	preSigners := map[string]xdr.AccountId{}
	postSigners := map[string]xdr.AccountId{}
	if change.Pre != nil {
		account := change.Pre.Data.MustAccount()
		preSigners = account.SponsorPerSigner()
	}
	if change.Post != nil {
		account := change.Post.Data.MustAccount()
		postSigners = account.SponsorPerSigner()
	}

	var all []string
	for signer := range preSigners {
		all = append(all, signer)
	}
	for signer := range postSigners {
		if _, ok := preSigners[signer]; ok {
			continue
		}
		all = append(all, signer)
	}
	sort.Strings(all)

	for _, signer := range all {
		pre, foundPre := preSigners[signer]
		post, foundPost := postSigners[signer]
		details := map[string]interface{}{}

		switch {
		case !foundPre && !foundPost:
			continue
		case !foundPre && foundPost:
			details["sponsor"] = post.Address()
			details["signer"] = signer
			srcAccount := change.Post.Data.MustAccount().AccountId
			e.addUnmuxed(&srcAccount, EffectSignerSponsorshipCreated, details)
		case !foundPost && foundPre:
			details["former_sponsor"] = pre.Address()
			details["signer"] = signer
			srcAccount := change.Pre.Data.MustAccount().AccountId
			e.addUnmuxed(&srcAccount, EffectSignerSponsorshipRemoved, details)
		case foundPre && foundPost:
			formerSponsor := pre.Address()
			newSponsor := post.Address()
			if formerSponsor == newSponsor {
				continue
			}

			details["former_sponsor"] = formerSponsor
			details["new_sponsor"] = newSponsor
			details["signer"] = signer
			srcAccount := change.Post.Data.MustAccount().AccountId
			e.addUnmuxed(&srcAccount, EffectSignerSponsorshipUpdated, details)
		}
	}
}

func (e *effectsWrapper) addLedgerEntrySponsorshipEffects(change ingest.Change) error {
	effectsForEntryType, found := sponsoringEffectsTable[change.Type]
	if !found {
		return nil
	}

	details := map[string]interface{}{}
	var effectType EffectType

	switch {
	case (change.Pre == nil || change.Pre.SponsoringID() == nil) &&
		(change.Post != nil && change.Post.SponsoringID() != nil):
		effectType = effectsForEntryType.created
		details["sponsor"] = (*change.Post.SponsoringID()).Address()
	case (change.Pre != nil && change.Pre.SponsoringID() != nil) &&
		(change.Post == nil || change.Post.SponsoringID() == nil):
		effectType = effectsForEntryType.removed
		details["former_sponsor"] = (*change.Pre.SponsoringID()).Address()
	case (change.Pre != nil && change.Pre.SponsoringID() != nil) &&
		(change.Post != nil && change.Post.SponsoringID() != nil):
		preSponsor := (*change.Pre.SponsoringID()).Address()
		postSponsor := (*change.Post.SponsoringID()).Address()
		if preSponsor == postSponsor {
			return nil
		}
		effectType = effectsForEntryType.updated
		details["new_sponsor"] = postSponsor
		details["former_sponsor"] = preSponsor
	default:
		return nil
	}

	var (
		accountID    *xdr.AccountId
		muxedAccount *xdr.MuxedAccount
	)

	var data xdr.LedgerEntryData
	if change.Post != nil {
		data = change.Post.Data
	} else {
		data = change.Pre.Data
	}

	switch change.Type {
	case xdr.LedgerEntryTypeAccount:
		a := data.MustAccount().AccountId
		accountID = &a
	case xdr.LedgerEntryTypeTrustline:
		tl := data.MustTrustLine()
		accountID = &tl.AccountId
		if tl.Asset.Type == xdr.AssetTypeAssetTypePoolShare {
			details["asset_type"] = "liquidity_pool"
			details["liquidity_pool_id"] = PoolIDToString(*tl.Asset.LiquidityPoolId)
		} else {
			details["asset"] = tl.Asset.ToAsset().StringCanonical()
		}
	case xdr.LedgerEntryTypeData:
		muxedAccount = e.operation.SourceAccount()
		details["data_name"] = data.MustData().DataName
	case xdr.LedgerEntryTypeClaimableBalance:
		muxedAccount = e.operation.SourceAccount()
		var err error
		details["balance_id"], err = xdr.MarshalHex(data.MustClaimableBalance().BalanceId)
		if err != nil {
			return errors.Wrapf(err, "Invalid balanceId in change from op %d", e.operation.index)
		}
	case xdr.LedgerEntryTypeLiquidityPool:
		// liquidity pools cannot be sponsored
		fallthrough
	default:
		return errors.Errorf("invalid sponsorship ledger entry type %v", change.Type.String())
	}

	if accountID != nil {
		e.addUnmuxed(accountID, effectType, details)
	} else {
		e.addMuxed(muxedAccount, effectType, details)
	}

	return nil
}

func (e *effectsWrapper) addLedgerEntryLiquidityPoolEffects(change ingest.Change) error {
	if change.Type != xdr.LedgerEntryTypeLiquidityPool {
		return nil
	}
	var effectType EffectType

	var details map[string]interface{}
	switch {
	case change.Pre == nil && change.Post != nil:
		effectType = EffectLiquidityPoolCreated
		details = map[string]interface{}{
			"liquidity_pool": liquidityPoolDetails(change.Post.Data.LiquidityPool),
		}
	case change.Pre != nil && change.Post == nil:
		effectType = EffectLiquidityPoolRemoved
		poolID := change.Pre.Data.LiquidityPool.LiquidityPoolId
		details = map[string]interface{}{
			"liquidity_pool_id": PoolIDToString(poolID),
		}
	default:
		return nil
	}
	e.addMuxed(
		e.operation.SourceAccount(),
		effectType,
		details,
	)

	return nil
}

