package utils

import (
	"context"

	"github.com/stellar/go-stellar-sdk/historyarchive"
	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/support/storage"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// mainnet history archive URLs
var mainArchiveURLs = []string{
	"https://history.stellar.org/prd/core-live/core_live_001",
	"https://history.stellar.org/prd/core-live/core_live_002",
	"https://history.stellar.org/prd/core-live/core_live_003",
}

// testnet is only used for local testing with new Protocol features
var testArchiveURLs = []string{
	"https://history.stellar.org/prd/core-testnet/core_testnet_001",
	"https://history.stellar.org/prd/core-testnet/core_testnet_002",
	"https://history.stellar.org/prd/core-testnet/core_testnet_003",
}

// futrenet is used for testing new Protocol features
var futureArchiveURLs = []string{
	"http://history.stellar.org/dev/core-futurenet/core_futurenet_001",
	"http://history.stellar.org/dev/core-futurenet/core_futurenet_002",
	"http://history.stellar.org/dev/core-futurenet/core_futurenet_003",
}

type EnvironmentDetails struct {
	NetworkPassphrase string
	ArchiveURLs       []string
	BinaryPath        string
	CoreConfig        string
	Network           string
	CommonFlagValues  CommonFlagValues
}

// GetPassphrase returns the correct Network Passphrase based on env preference
func GetEnvironmentDetails(commonFlags CommonFlagValues) (details EnvironmentDetails) {
	if commonFlags.IsTest {
		// testnet passphrase to be used for testing
		details.NetworkPassphrase = network.TestNetworkPassphrase
		details.ArchiveURLs = testArchiveURLs
		details.BinaryPath = "/usr/bin/stellar-core"
		details.CoreConfig = "/etl/docker/stellar-core_testnet.cfg"
		details.Network = "testnet"
		details.CommonFlagValues = commonFlags
		return details
	} else if commonFlags.IsFuture {
		// details.NetworkPassphrase = network.FutureNetworkPassphrase
		details.NetworkPassphrase = "Test SDF Future Network ; October 2022"
		details.ArchiveURLs = futureArchiveURLs
		details.BinaryPath = "/usr/bin/stellar-core"
		details.CoreConfig = "/etl/docker/stellar-core_futurenet.cfg"
		details.Network = "futurenet"
		details.CommonFlagValues = commonFlags
		return details
	} else {
		// default: mainnet
		details.NetworkPassphrase = network.PublicNetworkPassphrase
		details.ArchiveURLs = mainArchiveURLs
		details.BinaryPath = "/usr/bin/stellar-core"
		details.CoreConfig = "/etl/docker/stellar-core.cfg"
		details.Network = "pubnet"
		details.CommonFlagValues = commonFlags
		return details
	}
}

type CaptiveCore interface {
	CreateCaptiveCoreBackend() (ledgerbackend.CaptiveStellarCore, error)
}

func (e EnvironmentDetails) CreateCaptiveCoreBackend() (*ledgerbackend.CaptiveStellarCore, error) {
	captiveCoreToml, err := ledgerbackend.NewCaptiveCoreTomlFromFile(
		e.CoreConfig,
		ledgerbackend.CaptiveCoreTomlParams{
			NetworkPassphrase:  e.NetworkPassphrase,
			HistoryArchiveURLs: e.ArchiveURLs,
			Strict:             true,
		},
	)
	if err != nil {
		return &ledgerbackend.CaptiveStellarCore{}, err
	}
	backend, err := ledgerbackend.NewCaptive(
		ledgerbackend.CaptiveCoreConfig{
			BinaryPath:         e.BinaryPath,
			Toml:               captiveCoreToml,
			NetworkPassphrase:  e.NetworkPassphrase,
			HistoryArchiveURLs: e.ArchiveURLs,
			UserAgent:          "stellar-etl/1.0.0",
		},
	)
	return backend, err
}

func (e EnvironmentDetails) GetUnboundedLedgerCloseMeta(end uint32) (xdr.LedgerCloseMeta, error) {
	ctx := context.Background()

	backend, err := e.CreateCaptiveCoreBackend()
	if err != nil {
		return xdr.LedgerCloseMeta{}, err
	}

	ledgerRange := ledgerbackend.UnboundedRange(end)

	err = backend.PrepareRange(ctx, ledgerRange)
	if err != nil {
		return xdr.LedgerCloseMeta{}, err
	}

	ledgerCloseMeta, err := backend.GetLedger(ctx, end)
	if err != nil {
		return xdr.LedgerCloseMeta{}, err
	}

	return ledgerCloseMeta, nil
}

func CreateHistoryArchiveClient(archiveURLS []string) (historyarchive.ArchiveInterface, error) {
	archiveOptions := historyarchive.ArchiveOptions{
		ConnectOptions: storage.ConnectOptions{
			UserAgent: "stellar-etl/1.0.0",
		},
	}
	return historyarchive.NewArchivePool(archiveURLS, archiveOptions)
}

// GetLatestLedgerSequence returns the latest ledger sequence
func GetLatestLedgerSequence(archiveURLs []string) (uint32, error) {
	client, err := CreateHistoryArchiveClient(archiveURLs)
	if err != nil {
		return 0, err
	}

	root, err := client.GetRootHAS()
	if err != nil {
		return 0, err
	}

	return root.CurrentLedger, nil
}
