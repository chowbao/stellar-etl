package utils

import (
	"context"
	"fmt"
	"time"

	"github.com/stellar/go-stellar-sdk/historyarchive"
	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/support/datastore"
	"github.com/stellar/go-stellar-sdk/xdr"
)

type historyArchiveBackend struct {
	client  historyarchive.ArchiveInterface
	ledgers map[uint32]*historyarchive.Ledger
}

func (h historyArchiveBackend) GetLatestLedgerSequence(ctx context.Context) (sequence uint32, err error) {
	root, err := h.client.GetRootHAS()
	if err != nil {
		return 0, err
	}
	return root.CurrentLedger, nil
}

func (h historyArchiveBackend) GetLedgers(ctx context.Context) (map[uint32]*historyarchive.Ledger, error) {

	return h.ledgers, nil
}

func (h historyArchiveBackend) GetLedgerArchive(ctx context.Context, sequence uint32) (historyarchive.Ledger, error) {
	ledger, ok := h.ledgers[sequence]
	if !ok {
		return historyarchive.Ledger{}, fmt.Errorf("ledger %d is missing from map", sequence)
	}

	historyLedger := historyarchive.Ledger{
		Header:            ledger.Header,
		Transaction:       ledger.Transaction,
		TransactionResult: ledger.TransactionResult,
	}

	return historyLedger, nil
}

func (h historyArchiveBackend) GetLedger(ctx context.Context, sequence uint32) (xdr.LedgerCloseMeta, error) {
	ledger, ok := h.ledgers[sequence]
	if !ok {
		return xdr.LedgerCloseMeta{}, fmt.Errorf("ledger %d is missing from map", sequence)
	}

	lcm := xdr.LedgerCloseMeta{
		V: 0,
		V0: &xdr.LedgerCloseMetaV0{
			LedgerHeader: ledger.Header,
			TxSet:        ledger.Transaction.TxSet,
		},
	}
	lcm.V0.TxProcessing = make([]xdr.TransactionResultMeta, len(ledger.TransactionResult.TxResultSet.Results))
	for i, result := range ledger.TransactionResult.TxResultSet.Results {
		lcm.V0.TxProcessing[i].Result = result
	}

	return lcm, nil
}

func (h historyArchiveBackend) PrepareRange(ctx context.Context, ledgerRange ledgerbackend.Range) error {
	return nil
}

func (h historyArchiveBackend) IsPrepared(ctx context.Context, ledgerRange ledgerbackend.Range) (bool, error) {
	return true, nil
}

func (h historyArchiveBackend) Close() error {
	return nil
}

func CreateBackend(start, end uint32, archiveURLs []string) (historyArchiveBackend, error) {
	client, err := CreateHistoryArchiveClient(archiveURLs)
	if err != nil {
		return historyArchiveBackend{}, err
	}

	root, err := client.GetRootHAS()
	if err != nil {
		return historyArchiveBackend{}, err
	}
	if err = ValidateLedgerRange(start, end, root.CurrentLedger); err != nil {
		return historyArchiveBackend{}, err
	}

	ledgers, err := client.GetLedgers(start, end)
	if err != nil {
		return historyArchiveBackend{}, err
	}
	return historyArchiveBackend{client: client, ledgers: ledgers}, nil
}

// CreateDatastore creates the datastore to interface with GCS
// TODO: this can be updated to use different cloud storage services in the future.
// For now only GCS works datastore.Datastore.
func CreateDatastore(ctx context.Context, env EnvironmentDetails) (datastore.DataStore, datastore.DataStoreConfig, error) {
	// These params are specific for GCS
	params := make(map[string]string)
	params["destination_bucket_path"] = env.CommonFlagValues.DatastorePath + "/" + env.Network
	dataStoreConfig := datastore.DataStoreConfig{
		Type:   "GCS",
		Params: params,
		// TODO: In the future these will come from a config file written by ledgerexporter
		// Hard code DataStoreSchema values for now
		Schema: datastore.DataStoreSchema{
			LedgersPerFile:    1,
			FilesPerPartition: 64000,
		},
	}

	datastore, error := datastore.NewDataStore(ctx, dataStoreConfig)
	return datastore, dataStoreConfig, error
}

// CreateLedgerBackend creates a ledger backend using captive core or datastore
// Defaults to using datastore
func CreateLedgerBackend(ctx context.Context, useCaptiveCore bool, env EnvironmentDetails) (ledgerbackend.LedgerBackend, error) {
	// Create ledger backend from captive core
	if useCaptiveCore {
		backend, err := env.CreateCaptiveCoreBackend()
		if err != nil {
			return nil, err
		}
		return backend, nil
	}

	dataStore, datastoreConfig, err := CreateDatastore(ctx, env)
	if err != nil {
		return nil, err
	}

	BSBackendConfig := ledgerbackend.BufferedStorageBackendConfig{
		BufferSize: env.CommonFlagValues.BufferSize,
		NumWorkers: env.CommonFlagValues.NumWorkers,
		RetryLimit: env.CommonFlagValues.RetryLimit,
		RetryWait:  time.Duration(env.CommonFlagValues.RetryWait) * time.Second,
	}

	var schema datastore.DataStoreSchema
	schema, err = datastore.LoadSchema(context.Background(), dataStore, datastoreConfig)

	backend, err := ledgerbackend.NewBufferedStorageBackend(BSBackendConfig, dataStore, schema)
	if err != nil {
		return nil, err
	}
	return backend, nil
}
