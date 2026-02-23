package transform

import (
	"encoding/base64"
	"fmt"
	"time"

	"github.com/stellar/go-stellar-sdk/amount"
	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/protocols/horizon/base"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/support/errors"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stellar/stellar-etl/v2/internal/toid"
)

// transactionOperationWrapper represents the data for a single operation within a transaction
type transactionOperationWrapper struct {
	index          uint32
	transaction    ingest.LedgerTransaction
	operation      xdr.Operation
	ledgerSequence uint32
	network        string
	ledgerClosed   time.Time
}

// ID returns the ID for the operation.
func (operation *transactionOperationWrapper) ID() int64 {
	return toid.New(
		int32(operation.ledgerSequence),
		int32(operation.transaction.Index),
		int32(operation.index+1),
	).ToInt64()
}

// Order returns the operation order.
func (operation *transactionOperationWrapper) Order() uint32 {
	return operation.index + 1
}

// TransactionID returns the id for the transaction related with this operation.
func (operation *transactionOperationWrapper) TransactionID() int64 {
	return toid.New(int32(operation.ledgerSequence), int32(operation.transaction.Index), 0).ToInt64()
}

// SourceAccount returns the operation's source account.
func (operation *transactionOperationWrapper) SourceAccount() *xdr.MuxedAccount {
	sourceAccount := operation.operation.SourceAccount
	if sourceAccount != nil {
		return sourceAccount
	} else {
		ret := operation.transaction.Envelope.SourceAccount()
		return &ret
	}
}

// OperationType returns the operation type.
func (operation *transactionOperationWrapper) OperationType() xdr.OperationType {
	return operation.operation.Body.Type
}

func (operation *transactionOperationWrapper) getSignerSponsorInChange(signerKey string, change ingest.Change) xdr.SponsorshipDescriptor {
	if change.Type != xdr.LedgerEntryTypeAccount || change.Post == nil {
		return nil
	}

	preSigners := map[string]xdr.AccountId{}
	if change.Pre != nil {
		account := change.Pre.Data.MustAccount()
		preSigners = account.SponsorPerSigner()
	}

	account := change.Post.Data.MustAccount()
	postSigners := account.SponsorPerSigner()

	pre, preFound := preSigners[signerKey]
	post, postFound := postSigners[signerKey]

	if !postFound {
		return nil
	}

	if preFound {
		formerSponsor := pre.Address()
		newSponsor := post.Address()
		if formerSponsor == newSponsor {
			return nil
		}
	}

	return &post
}

func (operation *transactionOperationWrapper) getSponsor() (*xdr.AccountId, error) {
	changes, err := operation.transaction.GetOperationChanges(operation.index)
	if err != nil {
		return nil, err
	}
	var signerKey string
	if setOps, ok := operation.operation.Body.GetSetOptionsOp(); ok && setOps.Signer != nil {
		signerKey = setOps.Signer.Key.Address()
	}

	for _, c := range changes {
		// Check Signer changes
		if signerKey != "" {
			if sponsorAccount := operation.getSignerSponsorInChange(signerKey, c); sponsorAccount != nil {
				return sponsorAccount, nil
			}
		}

		// Check Ledger key changes
		if c.Pre != nil || c.Post == nil {
			// We are only looking for entry creations denoting that a sponsor
			// is associated to the ledger entry of the operation.
			continue
		}
		if sponsorAccount := c.Post.SponsoringID(); sponsorAccount != nil {
			return sponsorAccount, nil
		}
	}

	return nil, nil
}

var errLiquidityPoolChangeNotFound = errors.New("liquidity pool change not found")

func (operation *transactionOperationWrapper) getLiquidityPoolAndProductDelta(lpID *xdr.PoolId) (*xdr.LiquidityPoolEntry, *liquidityPoolDelta, error) {
	changes, err := operation.transaction.GetOperationChanges(operation.index)
	if err != nil {
		return nil, nil, err
	}

	for _, c := range changes {
		if c.Type != xdr.LedgerEntryTypeLiquidityPool {
			continue
		}
		// The delta can be caused by a full removal or full creation of the liquidity pool
		var lp *xdr.LiquidityPoolEntry
		var preA, preB, preShares xdr.Int64
		if c.Pre != nil {
			if lpID != nil && c.Pre.Data.LiquidityPool.LiquidityPoolId != *lpID {
				// if we were looking for specific pool id, then check on it
				continue
			}
			lp = c.Pre.Data.LiquidityPool
			if c.Pre.Data.LiquidityPool.Body.Type != xdr.LiquidityPoolTypeLiquidityPoolConstantProduct {
				return nil, nil, fmt.Errorf("unexpected liquity pool body type %d", c.Pre.Data.LiquidityPool.Body.Type)
			}
			cpPre := c.Pre.Data.LiquidityPool.Body.ConstantProduct
			preA, preB, preShares = cpPre.ReserveA, cpPre.ReserveB, cpPre.TotalPoolShares
		}
		var postA, postB, postShares xdr.Int64
		if c.Post != nil {
			if lpID != nil && c.Post.Data.LiquidityPool.LiquidityPoolId != *lpID {
				// if we were looking for specific pool id, then check on it
				continue
			}
			lp = c.Post.Data.LiquidityPool
			if c.Post.Data.LiquidityPool.Body.Type != xdr.LiquidityPoolTypeLiquidityPoolConstantProduct {
				return nil, nil, fmt.Errorf("unexpected liquity pool body type %d", c.Post.Data.LiquidityPool.Body.Type)
			}
			cpPost := c.Post.Data.LiquidityPool.Body.ConstantProduct
			postA, postB, postShares = cpPost.ReserveA, cpPost.ReserveB, cpPost.TotalPoolShares
		}
		delta := &liquidityPoolDelta{
			ReserveA:        postA - preA,
			ReserveB:        postB - preB,
			TotalPoolShares: postShares - preShares,
		}
		return lp, delta, nil
	}

	return nil, nil, errLiquidityPoolChangeNotFound
}

// OperationResult returns the operation's result record
func (operation *transactionOperationWrapper) OperationResult() *xdr.OperationResultTr {
	results, _ := operation.transaction.Result.OperationResults()
	tr := results[operation.index].MustTr()
	return &tr
}

func (operation *transactionOperationWrapper) findInitatingBeginSponsoringOp() *transactionOperationWrapper {
	if !operation.transaction.Result.Successful() {
		// Failed transactions may not have a compliant sandwich structure
		// we can rely on (e.g. invalid nesting or a being operation with the wrong sponsoree ID)
		// and thus we bail out since we could return incorrect information.
		return nil
	}
	sponsoree := operation.SourceAccount().ToAccountId()
	operations := operation.transaction.Envelope.Operations()
	for i := int(operation.index) - 1; i >= 0; i-- {
		if beginOp, ok := operations[i].Body.GetBeginSponsoringFutureReservesOp(); ok &&
			beginOp.SponsoredId.Address() == sponsoree.Address() {
			result := *operation
			result.index = uint32(i)
			result.operation = operations[i]
			return &result
		}
	}
	return nil
}

// Details returns the operation details as a map which can be stored as JSON.
func (operation *transactionOperationWrapper) Details() (map[string]interface{}, error) {
	details := map[string]interface{}{}
	source := operation.SourceAccount()
	switch operation.OperationType() {
	case xdr.OperationTypeCreateAccount:
		op := operation.operation.Body.MustCreateAccountOp()
		addAccountAndMuxedAccountDetails(details, *source, "funder")
		details["account"] = op.Destination.Address()
		details["starting_balance"] = amount.String(op.StartingBalance)
	case xdr.OperationTypePayment:
		op := operation.operation.Body.MustPaymentOp()
		addAccountAndMuxedAccountDetails(details, *source, "from")
		addAccountAndMuxedAccountDetails(details, op.Destination, "to")
		details["amount"] = amount.String(op.Amount)
		addAssetDetails(details, op.Asset, "")
	case xdr.OperationTypePathPaymentStrictReceive:
		op := operation.operation.Body.MustPathPaymentStrictReceiveOp()
		addAccountAndMuxedAccountDetails(details, *source, "from")
		addAccountAndMuxedAccountDetails(details, op.Destination, "to")

		details["amount"] = amount.String(op.DestAmount)
		details["source_amount"] = amount.String(0)
		details["source_max"] = amount.String(op.SendMax)
		addAssetDetails(details, op.DestAsset, "")
		addAssetDetails(details, op.SendAsset, "source_")

		if operation.transaction.Result.Successful() {
			result := operation.OperationResult().MustPathPaymentStrictReceiveResult()
			details["source_amount"] = amount.String(result.SendAmount())
		}

		var path = make([]map[string]interface{}, len(op.Path))
		for i := range op.Path {
			path[i] = make(map[string]interface{})
			addAssetDetails(path[i], op.Path[i], "")
		}
		details["path"] = path

	case xdr.OperationTypePathPaymentStrictSend:
		op := operation.operation.Body.MustPathPaymentStrictSendOp()
		addAccountAndMuxedAccountDetails(details, *source, "from")
		addAccountAndMuxedAccountDetails(details, op.Destination, "to")

		details["amount"] = amount.String(0)
		details["source_amount"] = amount.String(op.SendAmount)
		details["destination_min"] = amount.String(op.DestMin)
		addAssetDetails(details, op.DestAsset, "")
		addAssetDetails(details, op.SendAsset, "source_")

		if operation.transaction.Result.Successful() {
			result := operation.OperationResult().MustPathPaymentStrictSendResult()
			details["amount"] = amount.String(result.DestAmount())
		}

		var path = make([]map[string]interface{}, len(op.Path))
		for i := range op.Path {
			path[i] = make(map[string]interface{})
			addAssetDetails(path[i], op.Path[i], "")
		}
		details["path"] = path
	case xdr.OperationTypeManageBuyOffer:
		op := operation.operation.Body.MustManageBuyOfferOp()
		details["offer_id"] = op.OfferId
		details["amount"] = amount.String(op.BuyAmount)
		details["price"] = op.Price.String()
		details["price_r"] = map[string]interface{}{
			"n": op.Price.N,
			"d": op.Price.D,
		}
		addAssetDetails(details, op.Buying, "buying_")
		addAssetDetails(details, op.Selling, "selling_")
	case xdr.OperationTypeManageSellOffer:
		op := operation.operation.Body.MustManageSellOfferOp()
		details["offer_id"] = op.OfferId
		details["amount"] = amount.String(op.Amount)
		details["price"] = op.Price.String()
		details["price_r"] = map[string]interface{}{
			"n": op.Price.N,
			"d": op.Price.D,
		}
		addAssetDetails(details, op.Buying, "buying_")
		addAssetDetails(details, op.Selling, "selling_")
	case xdr.OperationTypeCreatePassiveSellOffer:
		op := operation.operation.Body.MustCreatePassiveSellOfferOp()
		details["amount"] = amount.String(op.Amount)
		details["price"] = op.Price.String()
		details["price_r"] = map[string]interface{}{
			"n": op.Price.N,
			"d": op.Price.D,
		}
		addAssetDetails(details, op.Buying, "buying_")
		addAssetDetails(details, op.Selling, "selling_")
	case xdr.OperationTypeSetOptions:
		op := operation.operation.Body.MustSetOptionsOp()

		if op.InflationDest != nil {
			details["inflation_dest"] = op.InflationDest.Address()
		}

		if op.SetFlags != nil && *op.SetFlags > 0 {
			addAuthFlagDetails(details, xdr.AccountFlags(*op.SetFlags), "set")
		}

		if op.ClearFlags != nil && *op.ClearFlags > 0 {
			addAuthFlagDetails(details, xdr.AccountFlags(*op.ClearFlags), "clear")
		}

		if op.MasterWeight != nil {
			details["master_key_weight"] = *op.MasterWeight
		}

		if op.LowThreshold != nil {
			details["low_threshold"] = *op.LowThreshold
		}

		if op.MedThreshold != nil {
			details["med_threshold"] = *op.MedThreshold
		}

		if op.HighThreshold != nil {
			details["high_threshold"] = *op.HighThreshold
		}

		if op.HomeDomain != nil {
			details["home_domain"] = *op.HomeDomain
		}

		if op.Signer != nil {
			details["signer_key"] = op.Signer.Key.Address()
			details["signer_weight"] = op.Signer.Weight
		}
	case xdr.OperationTypeChangeTrust:
		op := operation.operation.Body.MustChangeTrustOp()
		if op.Line.Type == xdr.AssetTypeAssetTypePoolShare {
			if err := addLiquidityPoolAssetDetails(details, *op.Line.LiquidityPool); err != nil {
				return nil, err
			}
		} else {
			addAssetDetails(details, op.Line.ToAsset(), "")
			details["trustee"] = details["asset_issuer"]
		}
		addAccountAndMuxedAccountDetails(details, *source, "trustor")
		details["limit"] = amount.String(op.Limit)
	case xdr.OperationTypeAllowTrust:
		op := operation.operation.Body.MustAllowTrustOp()
		addAssetDetails(details, op.Asset.ToAsset(source.ToAccountId()), "")
		addAccountAndMuxedAccountDetails(details, *source, "trustee")
		details["trustor"] = op.Trustor.Address()
		details["authorize"] = xdr.TrustLineFlags(op.Authorize).IsAuthorized()
		authLiabilities := xdr.TrustLineFlags(op.Authorize).IsAuthorizedToMaintainLiabilitiesFlag()
		if authLiabilities {
			details["authorize_to_maintain_liabilities"] = authLiabilities
		}
		clawbackEnabled := xdr.TrustLineFlags(op.Authorize).IsClawbackEnabledFlag()
		if clawbackEnabled {
			details["clawback_enabled"] = clawbackEnabled
		}
	case xdr.OperationTypeAccountMerge:
		addAccountAndMuxedAccountDetails(details, *source, "account")
		addAccountAndMuxedAccountDetails(details, operation.operation.Body.MustDestination(), "into")
	case xdr.OperationTypeInflation:
		// no inflation details, presently
	case xdr.OperationTypeManageData:
		op := operation.operation.Body.MustManageDataOp()
		details["name"] = string(op.DataName)
		if op.DataValue != nil {
			details["value"] = base64.StdEncoding.EncodeToString(*op.DataValue)
		} else {
			details["value"] = nil
		}
	case xdr.OperationTypeBumpSequence:
		op := operation.operation.Body.MustBumpSequenceOp()
		details["bump_to"] = fmt.Sprintf("%d", op.BumpTo)
	case xdr.OperationTypeCreateClaimableBalance:
		op := operation.operation.Body.MustCreateClaimableBalanceOp()
		details["asset"] = op.Asset.StringCanonical()
		details["amount"] = amount.String(op.Amount)
		var claimants []Claimant
		for _, c := range op.Claimants {
			cv0 := c.MustV0()
			claimants = append(claimants, Claimant{
				Destination: cv0.Destination.Address(),
				Predicate:   cv0.Predicate,
			})
		}
		details["claimants"] = claimants
	case xdr.OperationTypeClaimClaimableBalance:
		op := operation.operation.Body.MustClaimClaimableBalanceOp()
		balanceID, err := xdr.MarshalHex(op.BalanceId)
		if err != nil {
			panic(fmt.Errorf("invalid balanceId in op: %d", operation.index))
		}
		details["balance_id"] = balanceID
		details["balance_id_strkey"] = op.BalanceId.MustEncodeToStrkey()
		addAccountAndMuxedAccountDetails(details, *source, "claimant")
	case xdr.OperationTypeBeginSponsoringFutureReserves:
		op := operation.operation.Body.MustBeginSponsoringFutureReservesOp()
		details["sponsored_id"] = op.SponsoredId.Address()
	case xdr.OperationTypeEndSponsoringFutureReserves:
		beginSponsorshipOp := operation.findInitatingBeginSponsoringOp()
		if beginSponsorshipOp != nil {
			beginSponsorshipSource := beginSponsorshipOp.SourceAccount()
			addAccountAndMuxedAccountDetails(details, *beginSponsorshipSource, "begin_sponsor")
		}
	case xdr.OperationTypeRevokeSponsorship:
		op := operation.operation.Body.MustRevokeSponsorshipOp()
		switch op.Type {
		case xdr.RevokeSponsorshipTypeRevokeSponsorshipLedgerEntry:
			if err := addLedgerKeyDetails(details, *op.LedgerKey); err != nil {
				return nil, err
			}
		case xdr.RevokeSponsorshipTypeRevokeSponsorshipSigner:
			details["signer_account_id"] = op.Signer.AccountId.Address()
			details["signer_key"] = op.Signer.SignerKey.Address()
		}
	case xdr.OperationTypeClawback:
		op := operation.operation.Body.MustClawbackOp()
		addAssetDetails(details, op.Asset, "")
		addAccountAndMuxedAccountDetails(details, op.From, "from")
		details["amount"] = amount.String(op.Amount)
	case xdr.OperationTypeClawbackClaimableBalance:
		op := operation.operation.Body.MustClawbackClaimableBalanceOp()
		balanceID, err := xdr.MarshalHex(op.BalanceId)
		if err != nil {
			panic(fmt.Errorf("invalid balanceId in op: %d", operation.index))
		}
		details["balance_id"] = balanceID
		details["balance_id_strkey"] = op.BalanceId.MustEncodeToStrkey()
	case xdr.OperationTypeSetTrustLineFlags:
		op := operation.operation.Body.MustSetTrustLineFlagsOp()
		details["trustor"] = op.Trustor.Address()
		addAssetDetails(details, op.Asset, "")
		if op.SetFlags > 0 {
			addTrustLineFlagDetails(details, xdr.TrustLineFlags(op.SetFlags), "set")
		}

		if op.ClearFlags > 0 {
			addTrustLineFlagDetails(details, xdr.TrustLineFlags(op.ClearFlags), "clear")
		}
	case xdr.OperationTypeLiquidityPoolDeposit:
		op := operation.operation.Body.MustLiquidityPoolDepositOp()
		var err error
		var poolIDStrkey string
		var (
			assetA, assetB         string
			depositedA, depositedB xdr.Int64
			sharesReceived         xdr.Int64
		)

		details["liquidity_pool_id"] = PoolIDToString(op.LiquidityPoolId)
		poolIDStrkey, err = strkey.Encode(strkey.VersionByteLiquidityPool, op.LiquidityPoolId[:])
		if err != nil {
			return details, err
		}
		details["liquidity_pool_id_strkey"] = poolIDStrkey

		if operation.transaction.Result.Successful() {
			// we will use the defaults (omitted asset and 0 amounts) if the transaction failed
			lp, delta, err := operation.getLiquidityPoolAndProductDelta(&op.LiquidityPoolId)
			if err != nil {
				return nil, err
			}
			params := lp.Body.ConstantProduct.Params
			assetA, assetB = params.AssetA.StringCanonical(), params.AssetB.StringCanonical()
			depositedA, depositedB = delta.ReserveA, delta.ReserveB
			sharesReceived = delta.TotalPoolShares
		}
		details["reserves_max"] = []base.AssetAmount{
			{Asset: assetA, Amount: amount.String(op.MaxAmountA)},
			{Asset: assetB, Amount: amount.String(op.MaxAmountB)},
		}
		details["min_price"] = op.MinPrice.String()
		details["min_price_r"] = map[string]interface{}{
			"n": op.MinPrice.N,
			"d": op.MinPrice.D,
		}
		details["max_price"] = op.MaxPrice.String()
		details["max_price_r"] = map[string]interface{}{
			"n": op.MaxPrice.N,
			"d": op.MaxPrice.D,
		}
		details["reserves_deposited"] = []base.AssetAmount{
			{Asset: assetA, Amount: amount.String(depositedA)},
			{Asset: assetB, Amount: amount.String(depositedB)},
		}
		details["shares_received"] = amount.String(sharesReceived)
	case xdr.OperationTypeLiquidityPoolWithdraw:
		op := operation.operation.Body.MustLiquidityPoolWithdrawOp()
		var err error
		var poolIDStrkey string
		var (
			assetA, assetB       string
			receivedA, receivedB xdr.Int64
		)

		details["liquidity_pool_id"] = PoolIDToString(op.LiquidityPoolId)
		poolIDStrkey, err = strkey.Encode(strkey.VersionByteLiquidityPool, op.LiquidityPoolId[:])
		if err != nil {
			return details, err
		}
		details["liquidity_pool_id_strkey"] = poolIDStrkey

		if operation.transaction.Result.Successful() {
			// we will use the defaults (omitted asset and 0 amounts) if the transaction failed
			lp, delta, err := operation.getLiquidityPoolAndProductDelta(&op.LiquidityPoolId)
			if err != nil {
				return nil, err
			}
			params := lp.Body.ConstantProduct.Params
			assetA, assetB = params.AssetA.StringCanonical(), params.AssetB.StringCanonical()
			receivedA, receivedB = -delta.ReserveA, -delta.ReserveB
		}
		details["reserves_min"] = []base.AssetAmount{
			{Asset: assetA, Amount: amount.String(op.MinAmountA)},
			{Asset: assetB, Amount: amount.String(op.MinAmountB)},
		}
		details["shares"] = amount.String(op.Amount)
		details["reserves_received"] = []base.AssetAmount{
			{Asset: assetA, Amount: amount.String(receivedA)},
			{Asset: assetB, Amount: amount.String(receivedB)},
		}
	case xdr.OperationTypeInvokeHostFunction:
		op := operation.operation.Body.MustInvokeHostFunctionOp()
		details["function"] = op.HostFunction.Type.String()

		switch op.HostFunction.Type {
		case xdr.HostFunctionTypeHostFunctionTypeInvokeContract:
			invokeArgs := op.HostFunction.MustInvokeContract()
			args := make([]xdr.ScVal, 0, len(invokeArgs.Args)+2)
			args = append(args, xdr.ScVal{Type: xdr.ScValTypeScvAddress, Address: &invokeArgs.ContractAddress})
			args = append(args, xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &invokeArgs.FunctionName})
			args = append(args, invokeArgs.Args...)

			details["type"] = "invoke_contract"

			contractId, err := invokeArgs.ContractAddress.String()
			if err != nil {
				return nil, err
			}

			transactionEnvelope := getTransactionV1Envelope(operation.transaction.Envelope)
			details["ledger_key_hash"] = ledgerKeyHashFromTxEnvelope(transactionEnvelope)
			details["contract_id"] = contractId
			details["contract_code_hash"] = contractCodeHashFromTxEnvelope(transactionEnvelope)

			details["parameters"], details["parameters_decoded"] = serializeParameters(args)
			details["parameters_json"], details["parameters_json_decoded"], err = serializeScValArray(args)
			if err != nil {
				return nil, err
			}

			if balanceChanges, err := operation.parseAssetBalanceChangesFromContractEvents(); err != nil {
				return nil, err
			} else {
				details["asset_balance_changes"] = balanceChanges
			}

		case xdr.HostFunctionTypeHostFunctionTypeCreateContract:
			args := op.HostFunction.MustCreateContract()
			details["type"] = "create_contract"

			transactionEnvelope := getTransactionV1Envelope(operation.transaction.Envelope)
			details["ledger_key_hash"] = ledgerKeyHashFromTxEnvelope(transactionEnvelope)
			details["contract_id"] = contractIdFromTxEnvelope(transactionEnvelope)
			details["contract_code_hash"] = contractCodeHashFromTxEnvelope(transactionEnvelope)

			preimageTypeMap := switchContractIdPreimageType(args.ContractIdPreimage)
			for key, val := range preimageTypeMap {
				if _, ok := preimageTypeMap[key]; ok {
					details[key] = val
				}
			}
		case xdr.HostFunctionTypeHostFunctionTypeUploadContractWasm:
			details["type"] = "upload_wasm"
			transactionEnvelope := getTransactionV1Envelope(operation.transaction.Envelope)
			details["ledger_key_hash"] = ledgerKeyHashFromTxEnvelope(transactionEnvelope)
			details["contract_code_hash"] = contractCodeHashFromTxEnvelope(transactionEnvelope)
		case xdr.HostFunctionTypeHostFunctionTypeCreateContractV2:
			var err error
			args := op.HostFunction.MustCreateContractV2()
			details["type"] = "create_contract_v2"

			transactionEnvelope := getTransactionV1Envelope(operation.transaction.Envelope)
			details["ledger_key_hash"] = ledgerKeyHashFromTxEnvelope(transactionEnvelope)
			details["contract_id"] = contractIdFromTxEnvelope(transactionEnvelope)
			details["contract_code_hash"] = contractCodeHashFromTxEnvelope(transactionEnvelope)

			details["parameters"], details["parameters_decoded"] = serializeParameters(args.ConstructorArgs)
			details["parameters_json"], details["parameters_json_decoded"], err = serializeScValArray(args.ConstructorArgs)
			if err != nil {
				return nil, err
			}

			preimageTypeMap := switchContractIdPreimageType(args.ContractIdPreimage)
			for key, val := range preimageTypeMap {
				if _, ok := preimageTypeMap[key]; ok {
					details[key] = val
				}
			}
		default:
			panic(fmt.Errorf("unknown host function type: %s", op.HostFunction.Type))
		}
	case xdr.OperationTypeExtendFootprintTtl:
		op := operation.operation.Body.MustExtendFootprintTtlOp()
		details["type"] = "extend_footprint_ttl"
		details["extend_to"] = op.ExtendTo

		transactionEnvelope := getTransactionV1Envelope(operation.transaction.Envelope)
		details["ledger_key_hash"] = ledgerKeyHashFromTxEnvelope(transactionEnvelope)
		details["contract_id"] = contractIdFromTxEnvelope(transactionEnvelope)
		details["contract_code_hash"] = contractCodeHashFromTxEnvelope(transactionEnvelope)
	case xdr.OperationTypeRestoreFootprint:
		details["type"] = "restore_footprint"

		transactionEnvelope := getTransactionV1Envelope(operation.transaction.Envelope)
		details["ledger_key_hash"] = ledgerKeyHashFromTxEnvelope(transactionEnvelope)
		details["contract_id"] = contractIdFromTxEnvelope(transactionEnvelope)
		details["contract_code_hash"] = contractCodeHashFromTxEnvelope(transactionEnvelope)
	default:
		panic(fmt.Errorf("unknown operation type: %s", operation.OperationType()))
	}

	sponsor, err := operation.getSponsor()
	if err != nil {
		return nil, err
	}
	if sponsor != nil {
		details["sponsor"] = sponsor.Address()
	}

	return details, nil
}

// Participants returns the accounts taking part in the operation.
func (operation *transactionOperationWrapper) Participants() ([]xdr.AccountId, error) {
	participants := []xdr.AccountId{}
	participants = append(participants, operation.SourceAccount().ToAccountId())
	op := operation.operation

	switch operation.OperationType() {
	case xdr.OperationTypeCreateAccount:
		participants = append(participants, op.Body.MustCreateAccountOp().Destination)
	case xdr.OperationTypePayment:
		participants = append(participants, op.Body.MustPaymentOp().Destination.ToAccountId())
	case xdr.OperationTypePathPaymentStrictReceive:
		participants = append(participants, op.Body.MustPathPaymentStrictReceiveOp().Destination.ToAccountId())
	case xdr.OperationTypePathPaymentStrictSend:
		participants = append(participants, op.Body.MustPathPaymentStrictSendOp().Destination.ToAccountId())
	case xdr.OperationTypeManageBuyOffer:
		// the only direct participant is the source_account
	case xdr.OperationTypeManageSellOffer:
		// the only direct participant is the source_account
	case xdr.OperationTypeCreatePassiveSellOffer:
		// the only direct participant is the source_account
	case xdr.OperationTypeSetOptions:
		// the only direct participant is the source_account
	case xdr.OperationTypeChangeTrust:
		// the only direct participant is the source_account
	case xdr.OperationTypeAllowTrust:
		participants = append(participants, op.Body.MustAllowTrustOp().Trustor)
	case xdr.OperationTypeAccountMerge:
		participants = append(participants, op.Body.MustDestination().ToAccountId())
	case xdr.OperationTypeInflation:
		// the only direct participant is the source_account
	case xdr.OperationTypeManageData:
		// the only direct participant is the source_account
	case xdr.OperationTypeBumpSequence:
		// the only direct participant is the source_account
	case xdr.OperationTypeCreateClaimableBalance:
		for _, c := range op.Body.MustCreateClaimableBalanceOp().Claimants {
			participants = append(participants, c.MustV0().Destination)
		}
	case xdr.OperationTypeClaimClaimableBalance:
		// the only direct participant is the source_account
	case xdr.OperationTypeBeginSponsoringFutureReserves:
		participants = append(participants, op.Body.MustBeginSponsoringFutureReservesOp().SponsoredId)
	case xdr.OperationTypeEndSponsoringFutureReserves:
		beginSponsorshipOp := operation.findInitatingBeginSponsoringOp()
		if beginSponsorshipOp != nil {
			participants = append(participants, beginSponsorshipOp.SourceAccount().ToAccountId())
		}
	case xdr.OperationTypeRevokeSponsorship:
		op := operation.operation.Body.MustRevokeSponsorshipOp()
		switch op.Type {
		case xdr.RevokeSponsorshipTypeRevokeSponsorshipLedgerEntry:
			participants = append(participants, getLedgerKeyParticipants(*op.LedgerKey)...)
		case xdr.RevokeSponsorshipTypeRevokeSponsorshipSigner:
			participants = append(participants, op.Signer.AccountId)
			// We don't add signer as a participant because a signer can be arbitrary account.
			// This can spam successful operations history of any account.
		}
	case xdr.OperationTypeClawback:
		op := operation.operation.Body.MustClawbackOp()
		participants = append(participants, op.From.ToAccountId())
	case xdr.OperationTypeClawbackClaimableBalance:
		// the only direct participant is the source_account
	case xdr.OperationTypeSetTrustLineFlags:
		op := operation.operation.Body.MustSetTrustLineFlagsOp()
		participants = append(participants, op.Trustor)
	case xdr.OperationTypeLiquidityPoolDeposit:
		// the only direct participant is the source_account
	case xdr.OperationTypeLiquidityPoolWithdraw:
		// the only direct participant is the source_account
	case xdr.OperationTypeInvokeHostFunction:
		// the only direct participant is the source_account
	case xdr.OperationTypeExtendFootprintTtl:
		// the only direct participant is the source_account
	case xdr.OperationTypeRestoreFootprint:
		// the only direct participant is the source_account
	default:
		return participants, fmt.Errorf("unknown operation type: %s", op.Body.Type)
	}

	sponsor, err := operation.getSponsor()
	if err != nil {
		return nil, err
	}
	if sponsor != nil {
		participants = append(participants, *sponsor)
	}

	return dedupeParticipants(participants), nil
}
