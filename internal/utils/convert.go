package utils

import (
	"encoding/hex"
	"errors"
	"math/big"
	"time"

	"github.com/stellar/go-stellar-sdk/hash"
	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// PanicOnError is a function that panics if the provided error is not nil
func PanicOnError(err error) {
	if err != nil {
		panic(err)
	}
}

// HashToHexString is utility function that converts and xdr.Hash type to a hex string
func HashToHexString(inputHash xdr.Hash) string {
	sliceHash := inputHash[:]
	hexString := hex.EncodeToString(sliceHash)
	return hexString
}

// TimePointToUTCTimeStamp takes in an xdr TimePoint and converts it to a time.Time struct in UTC. It returns an error for negative timepoints
func TimePointToUTCTimeStamp(providedTime xdr.TimePoint) (time.Time, error) {
	intTime := int64(providedTime)
	if intTime < 0 {
		return time.Now(), errors.New("the timepoint is negative")
	}
	return time.Unix(intTime, 0).UTC(), nil
}

// GetAccountAddressFromMuxedAccount takes in a muxed account and returns the address of the account
func GetAccountAddressFromMuxedAccount(account xdr.MuxedAccount) (string, error) {
	providedID := account.ToAccountId()
	pointerToID := &providedID
	return pointerToID.GetAddress()
}

// ConvertStroopValueToReal converts a value in stroops, the smallest amount unit, into real units
func ConvertStroopValueToReal(input xdr.Int64) float64 {
	output, _ := big.NewRat(int64(input), int64(10000000)).Float64()
	return output
}

func LedgerEntryToLedgerKeyHash(ledgerEntry xdr.LedgerEntry) string {
	ledgerKey, _ := ledgerEntry.LedgerKey()
	ledgerKeyByte, _ := ledgerKey.MarshalBinary()
	hashedLedgerKeyByte := hash.Hash(ledgerKeyByte)
	ledgerKeyHash := hex.EncodeToString(hashedLedgerKeyByte[:])

	return ledgerKeyHash
}

func LedgerKeyToLedgerKeyHash(ledgerKey xdr.LedgerKey) string {
	ledgerKeyByte, _ := ledgerKey.MarshalBinary()
	hashedLedgerKeyByte := hash.Hash(ledgerKeyByte)
	ledgerKeyHash := hex.EncodeToString(hashedLedgerKeyByte[:])

	return ledgerKeyHash
}

// AccountSignersChanged returns true if account signers have changed.
// Notice: this will return true on master key changes too!
func AccountSignersChanged(c ingest.Change) bool {
	if c.Type != xdr.LedgerEntryTypeAccount {
		panic("This should not be called on changes other than Account changes")
	}

	// New account so new master key (which is also a signer)
	if c.Pre == nil {
		return true
	}

	// Account merged. Account being merge can still have signers.
	// c.Pre != nil at this point.
	if c.Post == nil {
		return true
	}

	// c.Pre != nil && c.Post != nil at this point.
	preAccountEntry := c.Pre.Data.MustAccount()
	postAccountEntry := c.Post.Data.MustAccount()

	preSigners := preAccountEntry.SignerSummary()
	postSigners := postAccountEntry.SignerSummary()

	if len(preSigners) != len(postSigners) {
		return true
	}

	for postSigner, postWeight := range postSigners {
		preWeight, exist := preSigners[postSigner]
		if !exist {
			return true
		}

		if preWeight != postWeight {
			return true
		}
	}

	preSignerSponsors := preAccountEntry.SignerSponsoringIDs()
	postSignerSponsors := postAccountEntry.SignerSponsoringIDs()

	if len(preSignerSponsors) != len(postSignerSponsors) {
		return true
	}

	for i := 0; i < len(preSignerSponsors); i++ {
		preSponsor := preSignerSponsors[i]
		postSponsor := postSignerSponsors[i]

		if preSponsor == nil && postSponsor != nil {
			return true
		} else if preSponsor != nil && postSponsor == nil {
			return true
		} else if preSponsor != nil && postSponsor != nil {
			preSponsorAccountID := xdr.AccountId(*preSponsor)
			preSponsorAddress := preSponsorAccountID.Address()

			postSponsorAccountID := xdr.AccountId(*postSponsor)
			postSponsorAddress := postSponsorAccountID.Address()

			if preSponsorAddress != postSponsorAddress {
				return true
			}
		}
	}

	return false
}
