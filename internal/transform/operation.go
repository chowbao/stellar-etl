package transform

import (
	"encoding/base64"
	"fmt"
	"strconv"

	"github.com/guregu/null"
	"github.com/stellar/stellar-etl/v2/internal/toid"
	"github.com/stellar/stellar-etl/v2/internal/utils"

	"github.com/stellar/go-stellar-sdk/amount"
	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"
)

type liquidityPoolDelta struct {
	ReserveA        xdr.Int64
	ReserveB        xdr.Int64
	TotalPoolShares xdr.Int64
}

// TransformOperation converts an operation from the history archive ingestion system into a form suitable for BigQuery
func TransformOperation(operation xdr.Operation, operationIndex int32, transaction ingest.LedgerTransaction, ledgerSeq int32, ledgerCloseMeta xdr.LedgerCloseMeta, network string) (OperationOutput, error) {
	outputTransactionID := toid.New(ledgerSeq, int32(transaction.Index), 0).ToInt64()
	outputOperationID := toid.New(ledgerSeq, int32(transaction.Index), operationIndex+1).ToInt64() //operationIndex needs +1 increment to stay in sync with ingest package

	sourceAccount := getOperationSourceAccount(operation, transaction)
	outputSourceAccount, err := utils.GetAccountAddressFromMuxedAccount(sourceAccount)
	if err != nil {
		return OperationOutput{}, fmt.Errorf("for operation %d (ledger id=%d): %v", operationIndex, outputOperationID, err)
	}

	var outputSourceAccountMuxed null.String
	if sourceAccount.Type == xdr.CryptoKeyTypeKeyTypeMuxedEd25519 {
		muxedAddress, err := sourceAccount.GetAddress()
		if err != nil {
			return OperationOutput{}, err
		}
		outputSourceAccountMuxed = null.StringFrom(muxedAddress)
	}

	outputOperationType := int32(operation.Body.Type)
	if outputOperationType < 0 {
		return OperationOutput{}, fmt.Errorf("the operation type (%d) is negative for  operation %d (operation id=%d)", outputOperationType, operationIndex, outputOperationID)
	}

	outputDetails, err := extractOperationDetails(operation, transaction, operationIndex, network)
	if err != nil {
		return OperationOutput{}, err
	}

	outputOperationTypeString, err := mapOperationType(operation)
	if err != nil {
		return OperationOutput{}, err
	}

	outputCloseTime, err := utils.GetCloseTime(ledgerCloseMeta)
	if err != nil {
		return OperationOutput{}, err
	}

	var outputOperationResultCode string
	var outputOperationTraceCode string
	outputOperationResults, ok := transaction.Result.Result.OperationResults()
	if ok {
		outputOperationResultCode = outputOperationResults[operationIndex].Code.String()
		operationResultTr, ok := outputOperationResults[operationIndex].GetTr()
		if ok {
			outputOperationTraceCode, err = mapOperationTrace(operationResultTr)
			if err != nil {
				return OperationOutput{}, err
			}
		}
	}

	outputLedgerSequence := utils.GetLedgerSequence(ledgerCloseMeta)

	transformedOperation := OperationOutput{
		SourceAccount:        outputSourceAccount,
		SourceAccountMuxed:   outputSourceAccountMuxed.String,
		Type:                 outputOperationType,
		TypeString:           outputOperationTypeString,
		TransactionID:        outputTransactionID,
		OperationID:          outputOperationID,
		OperationDetails:     outputDetails,
		ClosedAt:             outputCloseTime,
		OperationResultCode:  outputOperationResultCode,
		OperationTraceCode:   outputOperationTraceCode,
		LedgerSequence:       outputLedgerSequence,
		OperationDetailsJSON: outputDetails,
	}

	return transformedOperation, nil
}

func mapOperationType(operation xdr.Operation) (string, error) {
	var op_string_type string
	operationType := operation.Body.Type

	switch operationType {
	case xdr.OperationTypeCreateAccount:
		op_string_type = "create_account"
	case xdr.OperationTypePayment:
		op_string_type = "payment"
	case xdr.OperationTypePathPaymentStrictReceive:
		op_string_type = "path_payment_strict_receive"
	case xdr.OperationTypePathPaymentStrictSend:
		op_string_type = "path_payment_strict_send"
	case xdr.OperationTypeManageBuyOffer:
		op_string_type = "manage_buy_offer"
	case xdr.OperationTypeManageSellOffer:
		op_string_type = "manage_sell_offer"
	case xdr.OperationTypeCreatePassiveSellOffer:
		op_string_type = "create_passive_sell_offer"
	case xdr.OperationTypeSetOptions:
		op_string_type = "set_options"
	case xdr.OperationTypeChangeTrust:
		op_string_type = "change_trust"
	case xdr.OperationTypeAllowTrust:
		op_string_type = "allow_trust"
	case xdr.OperationTypeAccountMerge:
		op_string_type = "account_merge"
	case xdr.OperationTypeInflation:
		op_string_type = "inflation"
	case xdr.OperationTypeManageData:
		op_string_type = "manage_data"
	case xdr.OperationTypeBumpSequence:
		op_string_type = "bump_sequence"
	case xdr.OperationTypeCreateClaimableBalance:
		op_string_type = "create_claimable_balance"
	case xdr.OperationTypeClaimClaimableBalance:
		op_string_type = "claim_claimable_balance"
	case xdr.OperationTypeBeginSponsoringFutureReserves:
		op_string_type = "begin_sponsoring_future_reserves"
	case xdr.OperationTypeEndSponsoringFutureReserves:
		op_string_type = "end_sponsoring_future_reserves"
	case xdr.OperationTypeRevokeSponsorship:
		op_string_type = "revoke_sponsorship"
	case xdr.OperationTypeClawback:
		op_string_type = "clawback"
	case xdr.OperationTypeClawbackClaimableBalance:
		op_string_type = "clawback_claimable_balance"
	case xdr.OperationTypeSetTrustLineFlags:
		op_string_type = "set_trust_line_flags"
	case xdr.OperationTypeLiquidityPoolDeposit:
		op_string_type = "liquidity_pool_deposit"
	case xdr.OperationTypeLiquidityPoolWithdraw:
		op_string_type = "liquidity_pool_withdraw"
	case xdr.OperationTypeInvokeHostFunction:
		op_string_type = "invoke_host_function"
	case xdr.OperationTypeExtendFootprintTtl:
		op_string_type = "extend_footprint_ttl"
	case xdr.OperationTypeRestoreFootprint:
		op_string_type = "restore_footprint"
	default:
		return op_string_type, fmt.Errorf("unknown operation type: %s", operation.Body.Type.String())
	}
	return op_string_type, nil
}

func mapOperationTrace(operationTrace xdr.OperationResultTr) (string, error) {
	var operationTraceDescription string
	operationType := operationTrace.Type

	switch operationType {
	case xdr.OperationTypeCreateAccount:
		operationTraceDescription = operationTrace.CreateAccountResult.Code.String()
	case xdr.OperationTypePayment:
		operationTraceDescription = operationTrace.PaymentResult.Code.String()
	case xdr.OperationTypePathPaymentStrictReceive:
		operationTraceDescription = operationTrace.PathPaymentStrictReceiveResult.Code.String()
	case xdr.OperationTypePathPaymentStrictSend:
		operationTraceDescription = operationTrace.PathPaymentStrictSendResult.Code.String()
	case xdr.OperationTypeManageBuyOffer:
		operationTraceDescription = operationTrace.ManageBuyOfferResult.Code.String()
	case xdr.OperationTypeManageSellOffer:
		operationTraceDescription = operationTrace.ManageSellOfferResult.Code.String()
	case xdr.OperationTypeCreatePassiveSellOffer:
		operationTraceDescription = operationTrace.CreatePassiveSellOfferResult.Code.String()
	case xdr.OperationTypeSetOptions:
		operationTraceDescription = operationTrace.SetOptionsResult.Code.String()
	case xdr.OperationTypeChangeTrust:
		operationTraceDescription = operationTrace.ChangeTrustResult.Code.String()
	case xdr.OperationTypeAllowTrust:
		operationTraceDescription = operationTrace.AllowTrustResult.Code.String()
	case xdr.OperationTypeAccountMerge:
		operationTraceDescription = operationTrace.AccountMergeResult.Code.String()
	case xdr.OperationTypeInflation:
		operationTraceDescription = operationTrace.InflationResult.Code.String()
	case xdr.OperationTypeManageData:
		operationTraceDescription = operationTrace.ManageDataResult.Code.String()
	case xdr.OperationTypeBumpSequence:
		operationTraceDescription = operationTrace.BumpSeqResult.Code.String()
	case xdr.OperationTypeCreateClaimableBalance:
		operationTraceDescription = operationTrace.CreateClaimableBalanceResult.Code.String()
	case xdr.OperationTypeClaimClaimableBalance:
		operationTraceDescription = operationTrace.ClaimClaimableBalanceResult.Code.String()
	case xdr.OperationTypeBeginSponsoringFutureReserves:
		operationTraceDescription = operationTrace.BeginSponsoringFutureReservesResult.Code.String()
	case xdr.OperationTypeEndSponsoringFutureReserves:
		operationTraceDescription = operationTrace.EndSponsoringFutureReservesResult.Code.String()
	case xdr.OperationTypeRevokeSponsorship:
		operationTraceDescription = operationTrace.RevokeSponsorshipResult.Code.String()
	case xdr.OperationTypeClawback:
		operationTraceDescription = operationTrace.ClawbackResult.Code.String()
	case xdr.OperationTypeClawbackClaimableBalance:
		operationTraceDescription = operationTrace.ClawbackClaimableBalanceResult.Code.String()
	case xdr.OperationTypeSetTrustLineFlags:
		operationTraceDescription = operationTrace.SetTrustLineFlagsResult.Code.String()
	case xdr.OperationTypeLiquidityPoolDeposit:
		operationTraceDescription = operationTrace.LiquidityPoolDepositResult.Code.String()
	case xdr.OperationTypeLiquidityPoolWithdraw:
		operationTraceDescription = operationTrace.LiquidityPoolWithdrawResult.Code.String()
	case xdr.OperationTypeInvokeHostFunction:
		operationTraceDescription = operationTrace.InvokeHostFunctionResult.Code.String()
	case xdr.OperationTypeExtendFootprintTtl:
		operationTraceDescription = operationTrace.ExtendFootprintTtlResult.Code.String()
	case xdr.OperationTypeRestoreFootprint:
		operationTraceDescription = operationTrace.RestoreFootprintResult.Code.String()
	default:
		return operationTraceDescription, fmt.Errorf("unknown operation type: %s", operationTrace.Type.String())
	}
	return operationTraceDescription, nil
}

func PoolIDToString(id xdr.PoolId) string {
	return xdr.Hash(id).HexString()
}

// operation xdr.Operation, operationIndex int32, transaction ingest.LedgerTransaction, ledgerSeq int32
func getLiquidityPoolAndProductDelta(operationIndex int32, transaction ingest.LedgerTransaction, lpID *xdr.PoolId) (*xdr.LiquidityPoolEntry, *liquidityPoolDelta, error) {
	changes, err := transaction.GetOperationChanges(uint32(operationIndex))
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

	return nil, nil, fmt.Errorf("liquidity pool change not found")
}

func getOperationSourceAccount(operation xdr.Operation, transaction ingest.LedgerTransaction) xdr.MuxedAccount {
	sourceAccount := operation.SourceAccount
	if sourceAccount != nil {
		return *sourceAccount
	}

	return transaction.Envelope.SourceAccount()
}

func getSponsor(operation xdr.Operation, transaction ingest.LedgerTransaction, operationIndex int32) (*xdr.AccountId, error) {
	changes, err := transaction.GetOperationChanges(uint32(operationIndex))
	if err != nil {
		return nil, err
	}
	var signerKey string
	if setOps, ok := operation.Body.GetSetOptionsOp(); ok && setOps.Signer != nil {
		signerKey = setOps.Signer.Key.Address()
	}

	for _, c := range changes {
		// Check Signer changes
		if signerKey != "" {
			if sponsorAccount := getSignerSponsorInChange(signerKey, c); sponsorAccount != nil {
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

func getSignerSponsorInChange(signerKey string, change ingest.Change) xdr.SponsorshipDescriptor {
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

func extractOperationDetails(operation xdr.Operation, transaction ingest.LedgerTransaction, operationIndex int32, network string) (map[string]interface{}, error) {
	details := map[string]interface{}{}
	sourceAccount := getOperationSourceAccount(operation, transaction)
	operationType := operation.Body.Type

	switch operationType {
	case xdr.OperationTypeCreateAccount:
		op, ok := operation.Body.GetCreateAccountOp()
		if !ok {
			return details, fmt.Errorf("could not access CreateAccount info for this operation (index %d)", operationIndex)
		}

		if err := addAccountAndMuxedAccountDetails(details, sourceAccount, "funder"); err != nil {
			return details, err
		}
		details["account"] = op.Destination.Address()
		details["starting_balance"] = utils.ConvertStroopValueToReal(op.StartingBalance)

	case xdr.OperationTypePayment:
		op, ok := operation.Body.GetPaymentOp()
		if !ok {
			return details, fmt.Errorf("could not access Payment info for this operation (index %d)", operationIndex)
		}

		if err := addAccountAndMuxedAccountDetails(details, sourceAccount, "from"); err != nil {
			return details, err
		}
		if err := addAccountAndMuxedAccountDetails(details, op.Destination, "to"); err != nil {
			return details, err
		}
		details["amount"] = utils.ConvertStroopValueToReal(op.Amount)
		if err := addAssetDetailsToOperationDetails(details, op.Asset, ""); err != nil {
			return details, err
		}

	case xdr.OperationTypePathPaymentStrictReceive:
		op, ok := operation.Body.GetPathPaymentStrictReceiveOp()
		if !ok {
			return details, fmt.Errorf("could not access PathPaymentStrictReceive info for this operation (index %d)", operationIndex)
		}

		if err := addAccountAndMuxedAccountDetails(details, sourceAccount, "from"); err != nil {
			return details, err
		}
		if err := addAccountAndMuxedAccountDetails(details, op.Destination, "to"); err != nil {
			return details, err
		}
		details["amount"] = utils.ConvertStroopValueToReal(op.DestAmount)
		details["source_amount"] = amount.String(0)
		details["source_max"] = utils.ConvertStroopValueToReal(op.SendMax)
		if err := addAssetDetailsToOperationDetails(details, op.DestAsset, ""); err != nil {
			return details, err
		}
		if err := addAssetDetailsToOperationDetails(details, op.SendAsset, "source"); err != nil {
			return details, err
		}

		if transaction.Result.Successful() {
			allOperationResults, ok := transaction.Result.OperationResults()
			if !ok {
				return details, fmt.Errorf("could not access any results for this transaction")
			}
			currentOperationResult := allOperationResults[operationIndex]
			resultBody, ok := currentOperationResult.GetTr()
			if !ok {
				return details, fmt.Errorf("could not access result body for this operation (index %d)", operationIndex)
			}
			result, ok := resultBody.GetPathPaymentStrictReceiveResult()
			if !ok {
				return details, fmt.Errorf("could not access PathPaymentStrictReceive result info for this operation (index %d)", operationIndex)
			}
			details["source_amount"] = utils.ConvertStroopValueToReal(result.SendAmount())
		}

		details["path"] = transformPath(op.Path)

	case xdr.OperationTypePathPaymentStrictSend:
		op, ok := operation.Body.GetPathPaymentStrictSendOp()
		if !ok {
			return details, fmt.Errorf("could not access PathPaymentStrictSend info for this operation (index %d)", operationIndex)
		}

		if err := addAccountAndMuxedAccountDetails(details, sourceAccount, "from"); err != nil {
			return details, err
		}
		if err := addAccountAndMuxedAccountDetails(details, op.Destination, "to"); err != nil {
			return details, err
		}
		details["amount"] = amount.String(0)
		details["source_amount"] = utils.ConvertStroopValueToReal(op.SendAmount)
		details["destination_min"] = amount.String(op.DestMin)
		if err := addAssetDetailsToOperationDetails(details, op.DestAsset, ""); err != nil {
			return details, err
		}
		if err := addAssetDetailsToOperationDetails(details, op.SendAsset, "source"); err != nil {
			return details, err
		}

		if transaction.Result.Successful() {
			allOperationResults, ok := transaction.Result.OperationResults()
			if !ok {
				return details, fmt.Errorf("could not access any results for this transaction")
			}
			currentOperationResult := allOperationResults[operationIndex]
			resultBody, ok := currentOperationResult.GetTr()
			if !ok {
				return details, fmt.Errorf("could not access result body for this operation (index %d)", operationIndex)
			}
			result, ok := resultBody.GetPathPaymentStrictSendResult()
			if !ok {
				return details, fmt.Errorf("could not access GetPathPaymentStrictSendResult result info for this operation (index %d)", operationIndex)
			}
			details["amount"] = utils.ConvertStroopValueToReal(result.DestAmount())
		}

		details["path"] = transformPath(op.Path)

	case xdr.OperationTypeManageBuyOffer:
		op, ok := operation.Body.GetManageBuyOfferOp()
		if !ok {
			return details, fmt.Errorf("could not access ManageBuyOffer info for this operation (index %d)", operationIndex)
		}

		details["offer_id"] = int64(op.OfferId)
		details["amount"] = utils.ConvertStroopValueToReal(op.BuyAmount)
		if err := addPriceDetails(details, op.Price, ""); err != nil {
			return details, err
		}

		if err := addAssetDetailsToOperationDetails(details, op.Buying, "buying"); err != nil {
			return details, err
		}
		if err := addAssetDetailsToOperationDetails(details, op.Selling, "selling"); err != nil {
			return details, err
		}

	case xdr.OperationTypeManageSellOffer:
		op, ok := operation.Body.GetManageSellOfferOp()
		if !ok {
			return details, fmt.Errorf("could not access ManageSellOffer info for this operation (index %d)", operationIndex)
		}

		details["offer_id"] = int64(op.OfferId)
		details["amount"] = utils.ConvertStroopValueToReal(op.Amount)
		if err := addPriceDetails(details, op.Price, ""); err != nil {
			return details, err
		}

		if err := addAssetDetailsToOperationDetails(details, op.Buying, "buying"); err != nil {
			return details, err
		}
		if err := addAssetDetailsToOperationDetails(details, op.Selling, "selling"); err != nil {
			return details, err
		}

	case xdr.OperationTypeCreatePassiveSellOffer:
		op, ok := operation.Body.GetCreatePassiveSellOfferOp()
		if !ok {
			return details, fmt.Errorf("could not access CreatePassiveSellOffer info for this operation (index %d)", operationIndex)
		}

		details["amount"] = utils.ConvertStroopValueToReal(op.Amount)
		if err := addPriceDetails(details, op.Price, ""); err != nil {
			return details, err
		}

		if err := addAssetDetailsToOperationDetails(details, op.Buying, "buying"); err != nil {
			return details, err
		}
		if err := addAssetDetailsToOperationDetails(details, op.Selling, "selling"); err != nil {
			return details, err
		}

	case xdr.OperationTypeSetOptions:
		op, ok := operation.Body.GetSetOptionsOp()
		if !ok {
			return details, fmt.Errorf("could not access GetSetOptions info for this operation (index %d)", operationIndex)
		}

		if op.InflationDest != nil {
			details["inflation_dest"] = op.InflationDest.Address()
		}

		if op.SetFlags != nil && *op.SetFlags > 0 {
			addOperationFlagToOperationDetails(details, uint32(*op.SetFlags), "set")
		}

		if op.ClearFlags != nil && *op.ClearFlags > 0 {
			addOperationFlagToOperationDetails(details, uint32(*op.ClearFlags), "clear")
		}

		if op.MasterWeight != nil {
			details["master_key_weight"] = uint32(*op.MasterWeight)
		}

		if op.LowThreshold != nil {
			details["low_threshold"] = uint32(*op.LowThreshold)
		}

		if op.MedThreshold != nil {
			details["med_threshold"] = uint32(*op.MedThreshold)
		}

		if op.HighThreshold != nil {
			details["high_threshold"] = uint32(*op.HighThreshold)
		}

		if op.HomeDomain != nil {
			details["home_domain"] = string(*op.HomeDomain)
		}

		if op.Signer != nil {
			details["signer_key"] = op.Signer.Key.Address()
			details["signer_weight"] = uint32(op.Signer.Weight)
		}

	case xdr.OperationTypeChangeTrust:
		op, ok := operation.Body.GetChangeTrustOp()
		if !ok {
			return details, fmt.Errorf("could not access GetChangeTrust info for this operation (index %d)", operationIndex)
		}

		if op.Line.Type == xdr.AssetTypeAssetTypePoolShare {
			if err := addLiquidityPoolAssetDetails(details, *op.Line.LiquidityPool); err != nil {
				return details, err
			}
		} else {
			if err := addAssetDetailsToOperationDetails(details, op.Line.ToAsset(), ""); err != nil {
				return details, err
			}
			details["trustee"] = details["asset_issuer"]
		}

		if err := addAccountAndMuxedAccountDetails(details, sourceAccount, "trustor"); err != nil {
			return details, err
		}
		details["limit"] = utils.ConvertStroopValueToReal(op.Limit)

	case xdr.OperationTypeAllowTrust:
		op, ok := operation.Body.GetAllowTrustOp()
		if !ok {
			return details, fmt.Errorf("could not access AllowTrust info for this operation (index %d)", operationIndex)
		}

		if err := addAssetDetailsToOperationDetails(details, op.Asset.ToAsset(sourceAccount.ToAccountId()), ""); err != nil {
			return details, err
		}
		if err := addAccountAndMuxedAccountDetails(details, sourceAccount, "trustee"); err != nil {
			return details, err
		}
		details["trustor"] = op.Trustor.Address()
		shouldAuth := xdr.TrustLineFlags(op.Authorize).IsAuthorized()
		details["authorize"] = shouldAuth
		shouldAuthLiabilities := xdr.TrustLineFlags(op.Authorize).IsAuthorizedToMaintainLiabilitiesFlag()
		if shouldAuthLiabilities {
			details["authorize_to_maintain_liabilities"] = shouldAuthLiabilities
		}
		shouldClawbackEnabled := xdr.TrustLineFlags(op.Authorize).IsClawbackEnabledFlag()
		if shouldClawbackEnabled {
			details["clawback_enabled"] = shouldClawbackEnabled
		}

	case xdr.OperationTypeAccountMerge:
		destinationAccount, ok := operation.Body.GetDestination()
		if !ok {
			return details, fmt.Errorf("could not access Destination info for this operation (index %d)", operationIndex)
		}

		if err := addAccountAndMuxedAccountDetails(details, sourceAccount, "account"); err != nil {
			return details, err
		}
		if err := addAccountAndMuxedAccountDetails(details, destinationAccount, "into"); err != nil {
			return details, err
		}

	case xdr.OperationTypeInflation:
		// Inflation operations don't have information that affects the details struct
	case xdr.OperationTypeManageData:
		op, ok := operation.Body.GetManageDataOp()
		if !ok {
			return details, fmt.Errorf("could not access GetManageData info for this operation (index %d)", operationIndex)
		}

		details["name"] = string(op.DataName)
		if op.DataValue != nil {
			details["value"] = base64.StdEncoding.EncodeToString(*op.DataValue)
		} else {
			details["value"] = nil
		}

	case xdr.OperationTypeBumpSequence:
		op, ok := operation.Body.GetBumpSequenceOp()
		if !ok {
			return details, fmt.Errorf("could not access BumpSequence info for this operation (index %d)", operationIndex)
		}
		details["bump_to"] = fmt.Sprintf("%d", op.BumpTo)

	case xdr.OperationTypeCreateClaimableBalance:
		op := operation.Body.MustCreateClaimableBalanceOp()
		details["asset"] = op.Asset.StringCanonical()
		details["amount"] = utils.ConvertStroopValueToReal(op.Amount)
		details["claimants"] = transformClaimants(op.Claimants)

	case xdr.OperationTypeClaimClaimableBalance:
		op := operation.Body.MustClaimClaimableBalanceOp()
		balanceID, err := xdr.MarshalHex(op.BalanceId)
		if err != nil {
			return details, fmt.Errorf("invalid balanceId in op: %d", operationIndex)
		}
		details["balance_id"] = balanceID
		details["balance_id_strkey"] = op.BalanceId.MustEncodeToStrkey()
		if err := addAccountAndMuxedAccountDetails(details, sourceAccount, "claimant"); err != nil {
			return details, err
		}

	case xdr.OperationTypeBeginSponsoringFutureReserves:
		op := operation.Body.MustBeginSponsoringFutureReservesOp()
		details["sponsored_id"] = op.SponsoredId.Address()

	case xdr.OperationTypeEndSponsoringFutureReserves:
		beginSponsorOp := findInitatingBeginSponsoringOp(operation, operationIndex, transaction)
		if beginSponsorOp != nil {
			beginSponsorshipSource := getOperationSourceAccount(beginSponsorOp.Operation, transaction)
			if err := addAccountAndMuxedAccountDetails(details, beginSponsorshipSource, "begin_sponsor"); err != nil {
				return details, err
			}
		}

	case xdr.OperationTypeRevokeSponsorship:
		op := operation.Body.MustRevokeSponsorshipOp()
		switch op.Type {
		case xdr.RevokeSponsorshipTypeRevokeSponsorshipLedgerEntry:
			if err := addLedgerKeyToDetails(details, *op.LedgerKey); err != nil {
				return details, err
			}
		case xdr.RevokeSponsorshipTypeRevokeSponsorshipSigner:
			details["signer_account_id"] = op.Signer.AccountId.Address()
			details["signer_key"] = op.Signer.SignerKey.Address()
		}

	case xdr.OperationTypeClawback:
		op := operation.Body.MustClawbackOp()
		if err := addAssetDetailsToOperationDetails(details, op.Asset, ""); err != nil {
			return details, err
		}
		if err := addAccountAndMuxedAccountDetails(details, op.From, "from"); err != nil {
			return details, err
		}
		details["amount"] = utils.ConvertStroopValueToReal(op.Amount)

	case xdr.OperationTypeClawbackClaimableBalance:
		op := operation.Body.MustClawbackClaimableBalanceOp()
		balanceID, err := xdr.MarshalHex(op.BalanceId)
		if err != nil {
			return details, fmt.Errorf("invalid balanceId in op: %d", operationIndex)
		}
		details["balance_id"] = balanceID
		details["balance_id_strkey"] = op.BalanceId.MustEncodeToStrkey()

	case xdr.OperationTypeSetTrustLineFlags:
		op := operation.Body.MustSetTrustLineFlagsOp()
		details["trustor"] = op.Trustor.Address()
		if err := addAssetDetailsToOperationDetails(details, op.Asset, ""); err != nil {
			return details, err
		}
		if op.SetFlags > 0 {
			addTrustLineFlagToDetails(details, xdr.TrustLineFlags(op.SetFlags), "set")

		}
		if op.ClearFlags > 0 {
			addTrustLineFlagToDetails(details, xdr.TrustLineFlags(op.ClearFlags), "clear")
		}

	case xdr.OperationTypeLiquidityPoolDeposit:
		op := operation.Body.MustLiquidityPoolDepositOp()
		var err error
		var poolIDStrkey string
		var (
			assetA, assetB         xdr.Asset
			depositedA, depositedB xdr.Int64
			sharesReceived         xdr.Int64
		)

		details["liquidity_pool_id"] = PoolIDToString(op.LiquidityPoolId)
		poolIDStrkey, err = strkey.Encode(strkey.VersionByteLiquidityPool, op.LiquidityPoolId[:])
		if err != nil {
			return details, err
		}
		details["liquidity_pool_id_strkey"] = poolIDStrkey

		if transaction.Result.Successful() {
			// we will use the defaults (omitted asset and 0 amounts) if the transaction failed
			lp, delta, err := getLiquidityPoolAndProductDelta(operationIndex, transaction, &op.LiquidityPoolId)
			if err != nil {
				return nil, err
			}
			params := lp.Body.ConstantProduct.Params
			assetA, assetB = params.AssetA, params.AssetB
			depositedA, depositedB = delta.ReserveA, delta.ReserveB
			sharesReceived = delta.TotalPoolShares
		}

		// Process ReserveA Details
		if err := addAssetDetailsToOperationDetails(details, assetA, "reserve_a"); err != nil {
			return details, err
		}
		details["reserve_a_max_amount"] = utils.ConvertStroopValueToReal(op.MaxAmountA)
		depositA, err := strconv.ParseFloat(amount.String(depositedA), 64)
		if err != nil {
			return details, err
		}
		details["reserve_a_deposit_amount"] = depositA

		//Process ReserveB Details
		if err := addAssetDetailsToOperationDetails(details, assetB, "reserve_b"); err != nil {
			return details, err
		}
		details["reserve_b_max_amount"] = utils.ConvertStroopValueToReal(op.MaxAmountB)
		depositB, err := strconv.ParseFloat(amount.String(depositedB), 64)
		if err != nil {
			return details, err
		}
		details["reserve_b_deposit_amount"] = depositB

		if err := addPriceDetails(details, op.MinPrice, "min"); err != nil {
			return details, err
		}
		if err := addPriceDetails(details, op.MaxPrice, "max"); err != nil {
			return details, err
		}

		sharesToFloat, err := strconv.ParseFloat(amount.String(sharesReceived), 64)
		if err != nil {
			return details, err
		}
		details["shares_received"] = sharesToFloat

	case xdr.OperationTypeLiquidityPoolWithdraw:
		op := operation.Body.MustLiquidityPoolWithdrawOp()
		var err error
		var poolIDStrkey string
		var (
			assetA, assetB       xdr.Asset
			receivedA, receivedB xdr.Int64
		)

		details["liquidity_pool_id"] = PoolIDToString(op.LiquidityPoolId)
		poolIDStrkey, err = strkey.Encode(strkey.VersionByteLiquidityPool, op.LiquidityPoolId[:])
		if err != nil {
			return details, err
		}
		details["liquidity_pool_id_strkey"] = poolIDStrkey

		if transaction.Result.Successful() {
			// we will use the defaults (omitted asset and 0 amounts) if the transaction failed
			lp, delta, err := getLiquidityPoolAndProductDelta(operationIndex, transaction, &op.LiquidityPoolId)
			if err != nil {
				return nil, err
			}
			params := lp.Body.ConstantProduct.Params
			assetA, assetB = params.AssetA, params.AssetB
			receivedA, receivedB = -delta.ReserveA, -delta.ReserveB
		}
		// Process AssetA Details
		if err := addAssetDetailsToOperationDetails(details, assetA, "reserve_a"); err != nil {
			return details, err
		}
		details["reserve_a_min_amount"] = utils.ConvertStroopValueToReal(op.MinAmountA)
		details["reserve_a_withdraw_amount"] = utils.ConvertStroopValueToReal(receivedA)

		// Process AssetB Details
		if err := addAssetDetailsToOperationDetails(details, assetB, "reserve_b"); err != nil {
			return details, err
		}
		details["reserve_b_min_amount"] = utils.ConvertStroopValueToReal(op.MinAmountB)
		details["reserve_b_withdraw_amount"] = utils.ConvertStroopValueToReal(receivedB)

		details["shares"] = utils.ConvertStroopValueToReal(op.Amount)

	case xdr.OperationTypeInvokeHostFunction:
		op := operation.Body.MustInvokeHostFunctionOp()
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

			transactionEnvelope := getTransactionV1Envelope(transaction.Envelope)
			details["ledger_key_hash"] = ledgerKeyHashFromTxEnvelope(transactionEnvelope)
			details["contract_id"] = contractId
			details["contract_code_hash"] = contractCodeHashFromTxEnvelope(transactionEnvelope)

			details["parameters"], details["parameters_decoded"] = serializeParameters(args)
			details["parameters_json"], details["parameters_json_decoded"], err = serializeScValArray(args)
			if err != nil {
				return nil, err
			}

			if balanceChanges, err := parseAssetBalanceChangesFromContractEvents(transaction, network); err != nil {
				return nil, err
			} else {
				details["asset_balance_changes"] = balanceChanges
			}

		case xdr.HostFunctionTypeHostFunctionTypeCreateContract:
			args := op.HostFunction.MustCreateContract()
			details["type"] = "create_contract"

			transactionEnvelope := getTransactionV1Envelope(transaction.Envelope)
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
			transactionEnvelope := getTransactionV1Envelope(transaction.Envelope)
			details["ledger_key_hash"] = ledgerKeyHashFromTxEnvelope(transactionEnvelope)
			details["contract_code_hash"] = contractCodeHashFromTxEnvelope(transactionEnvelope)
		case xdr.HostFunctionTypeHostFunctionTypeCreateContractV2:
			var err error
			args := op.HostFunction.MustCreateContractV2()
			details["type"] = "create_contract_v2"

			transactionEnvelope := getTransactionV1Envelope(transaction.Envelope)
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
		op := operation.Body.MustExtendFootprintTtlOp()
		details["type"] = "extend_footprint_ttl"
		details["extend_to"] = op.ExtendTo

		transactionEnvelope := getTransactionV1Envelope(transaction.Envelope)
		details["ledger_key_hash"] = ledgerKeyHashFromTxEnvelope(transactionEnvelope)
		details["contract_id"] = contractIdFromTxEnvelope(transactionEnvelope)
		details["contract_code_hash"] = contractCodeHashFromTxEnvelope(transactionEnvelope)
	case xdr.OperationTypeRestoreFootprint:
		details["type"] = "restore_footprint"

		transactionEnvelope := getTransactionV1Envelope(transaction.Envelope)
		details["ledger_key_hash"] = ledgerKeyHashFromTxEnvelope(transactionEnvelope)
		details["contract_id"] = contractIdFromTxEnvelope(transactionEnvelope)
		details["contract_code_hash"] = contractCodeHashFromTxEnvelope(transactionEnvelope)
	default:
		return details, fmt.Errorf("unknown operation type: %s", operation.Body.Type.String())
	}

	sponsor, err := getSponsor(operation, transaction, operationIndex)
	if err != nil {
		return nil, err
	}
	if sponsor != nil {
		details["sponsor"] = sponsor.Address()
	}

	return details, nil
}
