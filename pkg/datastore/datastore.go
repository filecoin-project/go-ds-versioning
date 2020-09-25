package datastore

import (
	"context"

	"github.com/filecoin-project/go-ds-versioning/internal/migrate"
	"github.com/filecoin-project/go-ds-versioning/internal/runner"
	versioning "github.com/filecoin-project/go-ds-versioning/pkg"
	datastore "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"
	"github.com/ipfs/go-datastore/query"
)

type migratedDatastore struct {
	ds datastore.Batching
	ms versioning.MigrationState
}

// NewVersionedDatastore sets takes a datastore, migrations, list, and target version, and returns
// a datastore whose functions will fail till it's migrated to the target version and a function to run migrations
func NewVersionedDatastore(ds datastore.Batching, migrations versioning.VersionedMigrationList, target versioning.VersionKey) (datastore.Batching, func(context.Context) error) {
	r := runner.NewRunner(ds, migrations, target, migrate.To)
	return NewMigratedDatastore(namespace.Wrap(ds, datastore.NewKey(string(target))), r), r.Migrate
}

// NewMigratedDatastore returns a datastore whose functions will fail until the migration state says its ready
func NewMigratedDatastore(ds datastore.Batching, ms versioning.MigrationState) datastore.Batching {
	return &migratedDatastore{ds, ms}
}

func (ds *migratedDatastore) Get(key datastore.Key) (value []byte, err error) {
	if err := ds.ms.ReadyError(); err != nil {
		return nil, err
	}
	return ds.ds.Get(key)
}

func (ds *migratedDatastore) Has(key datastore.Key) (exists bool, err error) {
	if err := ds.ms.ReadyError(); err != nil {
		return false, err
	}
	return ds.ds.Has(key)
}

func (ds *migratedDatastore) GetSize(key datastore.Key) (size int, err error) {
	if err := ds.ms.ReadyError(); err != nil {
		return 0, err
	}
	return ds.ds.GetSize(key)
}

func (ds *migratedDatastore) Query(q query.Query) (query.Results, error) {
	if err := ds.ms.ReadyError(); err != nil {
		return nil, err
	}
	return ds.ds.Query(q)
}

func (ds *migratedDatastore) Put(key datastore.Key, value []byte) error {
	if err := ds.ms.ReadyError(); err != nil {
		return err
	}
	return ds.ds.Put(key, value)
}

func (ds *migratedDatastore) Delete(key datastore.Key) error {
	if err := ds.ms.ReadyError(); err != nil {
		return err
	}
	return ds.ds.Delete(key)
}

func (ds *migratedDatastore) Sync(prefix datastore.Key) error {
	if err := ds.ms.ReadyError(); err != nil {
		return err
	}
	return ds.ds.Sync(prefix)
}

func (ds *migratedDatastore) Close() error {
	if err := ds.ms.ReadyError(); err != nil {
		return err
	}
	return ds.ds.Close()
}

func (ds *migratedDatastore) Batch() (datastore.Batch, error) {
	if err := ds.ms.ReadyError(); err != nil {
		return nil, err
	}
	return ds.ds.Batch()
}

var _ datastore.Batching = &migratedDatastore{}
