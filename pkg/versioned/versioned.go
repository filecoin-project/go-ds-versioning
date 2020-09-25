package versioned

import (
	"context"

	versioning "github.com/filecoin-project/go-ds-versioning/pkg"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"
)

type migrateFunc func(ctx context.Context, ds1 datastore.Batching, ds2 datastore.Batching) ([]datastore.Key, error)

func versionMigrate(ctx context.Context, mf migrateFunc, ds datastore.Batching, from versioning.VersionKey, to versioning.VersionKey) ([]datastore.Key, error) {
	ds1 := namespace.Wrap(ds, datastore.NewKey(string(from)))
	ds2 := namespace.Wrap(ds, datastore.NewKey(string(to)))
	return mf(ctx, ds1, ds2)
}

type versionedMigration struct {
	oldKey    versioning.VersionKey
	newKey    versioning.VersionKey
	migration versioning.DatastoreMigration
}

func (vm versionedMigration) OldVersion() versioning.VersionKey {
	return vm.oldKey
}

func (vm versionedMigration) NewVersion() versioning.VersionKey {
	return vm.newKey
}

func (vm versionedMigration) Up(ctx context.Context, ds datastore.Batching) ([]datastore.Key, error) {
	return versionMigrate(ctx, vm.migration.Up, ds, vm.oldKey, vm.newKey)
}

type reversibleVersionedMigration struct {
	versionedMigration
}

func (rvm reversibleVersionedMigration) Down(ctx context.Context, ds datastore.Batching) ([]datastore.Key, error) {
	return versionMigrate(ctx, rvm.migration.(versioning.ReversableDatastoreMigration).Down, ds, rvm.newKey, rvm.oldKey)
}

// NewVersionedMigration converts a datastore migration to a versioned migration with the given old and new versions
func NewVersionedMigration(datastoreMigration versioning.DatastoreMigration, oldVersion versioning.VersionKey, newVersion versioning.VersionKey) versioning.VersionedMigration {
	vm := versionedMigration{oldVersion, newVersion, datastoreMigration}
	if _, ok := datastoreMigration.(versioning.ReversableDatastoreMigration); ok {
		return reversibleVersionedMigration{vm}
	}
	return vm
}

// NewInitialVersionedMigration sets up a migration that starts from an unversioned datastore
func NewInitialVersionedMigration(datastoreMigration versioning.DatastoreMigration, newVersion versioning.VersionKey) versioning.VersionedMigration {
	return NewVersionedMigration(datastoreMigration, "", newVersion)
}
