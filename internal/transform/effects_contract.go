package transform

import (
	"fmt"

	"github.com/guregu/null"
	"github.com/stellar/go-stellar-sdk/amount"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/support/contractevents"
	"github.com/stellar/go-stellar-sdk/support/errors"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// addInvokeHostFunctionEffects iterates through the events and generates
// account_credited and account_debited effects when it sees events related to
// the Stellar Asset Contract corresponding to those effects.
func (e *effectsWrapper) addInvokeHostFunctionEffects(events []contractevents.Event) error {
	if e.operation.network == "" {
		return errors.New("invokeHostFunction effects cannot be determined unless network passphrase is set")
	}

	source := e.operation.SourceAccount()
	for _, event := range events {
		evt, err := contractevents.NewStellarAssetContractEvent(&event, e.operation.network)
		if err != nil {
			continue // irrelevant or unsupported event
		}

		details := make(map[string]interface{}, 4)
		addAssetDetails(details, evt.GetAsset(), "")

		//
		// Note: We ignore effects that involve contracts (until the day we have
		// contract_debited/credited effects, may it never come :pray:)
		//

		switch evt.GetType() {
		// Transfer events generate an `account_debited` effect for the `from`
		// (sender) and an `account_credited` effect for the `to` (recipient).
		case contractevents.EventTypeTransfer:
			details["contract_event_type"] = "transfer"
			transferEvent := evt.(*contractevents.TransferEvent)
			details["amount"] = amount.String128(transferEvent.Amount)
			toDetails := map[string]interface{}{}
			for key, val := range details {
				toDetails[key] = val
			}

			if strkey.IsValidEd25519PublicKey(transferEvent.From) {
				e.add(
					transferEvent.From,
					null.String{},
					EffectAccountDebited,
					details,
				)
			} else {
				details["contract"] = transferEvent.From
				e.addMuxed(source, EffectContractDebited, details)
			}

			if strkey.IsValidEd25519PublicKey(transferEvent.To) {
				e.add(
					transferEvent.To,
					null.String{},
					EffectAccountCredited,
					toDetails,
				)
			} else {
				toDetails["contract"] = transferEvent.To
				e.addMuxed(source, EffectContractCredited, toDetails)
			}

		// Mint events imply a non-native asset, and it results in a credit to
		// the `to` recipient.
		case contractevents.EventTypeMint:
			details["contract_event_type"] = "mint"
			mintEvent := evt.(*contractevents.MintEvent)
			details["amount"] = amount.String128(mintEvent.Amount)
			if strkey.IsValidEd25519PublicKey(mintEvent.To) {
				e.add(
					mintEvent.To,
					null.String{},
					EffectAccountCredited,
					details,
				)
			} else {
				details["contract"] = mintEvent.To
				e.addMuxed(source, EffectContractCredited, details)
			}

		// Clawback events result in a debit to the `from` address, but acts
		// like a burn to the recipient, so these are functionally equivalent
		case contractevents.EventTypeClawback:
			details["contract_event_type"] = "clawback"
			cbEvent := evt.(*contractevents.ClawbackEvent)
			details["amount"] = amount.String128(cbEvent.Amount)
			if strkey.IsValidEd25519PublicKey(cbEvent.From) {
				e.add(
					cbEvent.From,
					null.String{},
					EffectAccountDebited,
					details,
				)
			} else {
				details["contract"] = cbEvent.From
				e.addMuxed(source, EffectContractDebited, details)
			}

		case contractevents.EventTypeBurn:
			details["contract_event_type"] = "burn"
			burnEvent := evt.(*contractevents.BurnEvent)
			details["amount"] = amount.String128(burnEvent.Amount)
			if strkey.IsValidEd25519PublicKey(burnEvent.From) {
				e.add(
					burnEvent.From,
					null.String{},
					EffectAccountDebited,
					details,
				)
			} else {
				details["contract"] = burnEvent.From
				e.addMuxed(source, EffectContractDebited, details)
			}
		}
	}

	return nil
}

func (e *effectsWrapper) addExtendFootprintTtlEffect() error {
	op := e.operation.operation.Body.MustExtendFootprintTtlOp()

	// Figure out which entries were affected
	changes, err := e.operation.transaction.GetOperationChanges(e.operation.index)
	if err != nil {
		return err
	}
	entries := make([]string, 0, len(changes))
	for _, change := range changes {
		// They should all have a post
		if change.Post == nil {
			return fmt.Errorf("invalid bump footprint expiration operation: %v", op)
		}
		var key xdr.LedgerKey
		switch change.Post.Data.Type {
		case xdr.LedgerEntryTypeTtl:
			v := change.Post.Data.MustTtl()
			if err := key.SetTtl(v.KeyHash); err != nil {
				return err
			}
		default:
			// Ignore any non-contract entries, as they couldn't have been affected.
			//
			// Should we error here? No, because there might be other entries
			// affected, for example, the user's balance.
			continue
		}
		b64, err := xdr.MarshalBase64(key)
		if err != nil {
			return err
		}
		entries = append(entries, b64)
	}
	details := map[string]interface{}{
		"entries":   entries,
		"extend_to": op.ExtendTo,
	}
	e.addMuxed(e.operation.SourceAccount(), EffectExtendFootprintTtl, details)
	return nil
}

func (e *effectsWrapper) addRestoreFootprintExpirationEffect() error {
	op := e.operation.operation.Body.MustRestoreFootprintOp()

	// Figure out which entries were affected
	changes, err := e.operation.transaction.GetOperationChanges(e.operation.index)
	if err != nil {
		return err
	}
	entries := make([]string, 0, len(changes))
	for _, change := range changes {
		// They should all have a post
		if change.Post == nil {
			return fmt.Errorf("invalid restore footprint operation: %v", op)
		}
		var key xdr.LedgerKey
		switch change.Post.Data.Type {
		case xdr.LedgerEntryTypeTtl:
			v := change.Post.Data.MustTtl()
			if err := key.SetTtl(v.KeyHash); err != nil {
				return err
			}
		default:
			// Ignore any non-contract entries, as they couldn't have been affected.
			//
			// Should we error here? No, because there might be other entries
			// affected, for example, the user's balance.
			continue
		}
		b64, err := xdr.MarshalBase64(key)
		if err != nil {
			return err
		}
		entries = append(entries, b64)
	}
	details := map[string]interface{}{
		"entries": entries,
	}
	e.addMuxed(e.operation.SourceAccount(), EffectRestoreFootprint, details)
	return nil
}
