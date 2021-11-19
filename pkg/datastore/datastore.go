// Package datastore provides an abstraction on top of go-datastore that allows
// you to make a datastore that tracks its own version and knows how to
// migrate itself to the target version
package datastore

import (
	"context"

	datastore "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"
	"github.com/ipfs/go-datastore/query"

	"github.com/filecoin-project/go-ds-versioning/internal/migrate"
	"github.com/filecoin-project/go-ds-versioning/internal/runner"
	versioning "github.com/filecoin-project/go-ds-versioning/pkg"
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

func (ds *migratedDatastore) Get(ctx context.Context, key datastore.Key) (value []byte, err error) {
	if err := ds.ms.ReadyError(); err != nil {
		return nil, err
	}
	return ds.ds.Get(ctx, key)
}

func (ds *migratedDatastore) Has(ctx context.Context, key datastore.Key) (exists bool, err error) {
	if err := ds.ms.ReadyError(); err != nil {
		return false, err
	}
	return ds.ds.Has(ctx, key)
}

func (ds *migratedDatastore) GetSize(ctx context.Context, key datastore.Key) (size int, err error) {
	if err := ds.ms.ReadyError(); err != nil {
		return 0, err
	}
	return ds.ds.GetSize(ctx, key)
}

func (ds *migratedDatastore) Query(ctx context.Context, q query.Query) (query.Results, error) {
	if err := ds.ms.ReadyError(); err != nil {
		return nil, err
	}
	return ds.ds.Query(ctx, q)
}

func (ds *migratedDatastore) Put(ctx context.Context, key datastore.Key, value []byte) error {
	if err := ds.ms.ReadyError(); err != nil {
		return err
	}
	return ds.ds.Put(ctx, key, value)
}

func (ds *migratedDatastore) Delete(ctx context.Context, key datastore.Key) error {
	if err := ds.ms.ReadyError(); err != nil {
		return err
	}
	return ds.ds.Delete(ctx, key)
}

func (ds *migratedDatastore) Sync(ctx context.Context, prefix datastore.Key) error {
	if err := ds.ms.ReadyError(); err != nil {
		return err
	}
	return ds.ds.Sync(ctx, prefix)
}

func (ds *migratedDatastore) Close() error {
	if err := ds.ms.ReadyError(); err != nil {
		return err
	}
	return ds.ds.Close()
}

func (ds *migratedDatastore) Batch(ctx context.Context) (datastore.Batch, error) {
	if err := ds.ms.ReadyError(); err != nil {
		return nil, err
	}
	return ds.ds.Batch(ctx)
}

var _ datastore.Batching = &migratedDatastore{}
