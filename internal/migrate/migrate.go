package migrate

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"sort"

	cborutil "github.com/filecoin-project/go-cbor-util"
	"github.com/filecoin-project/go-ds-versioning/internal/utils"
	versioning "github.com/filecoin-project/go-ds-versioning/pkg"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	cbg "github.com/whyrusleeping/cbor-gen"
	"go.uber.org/multierr"
)

// Execute executes a database migration from datastore to another, using the given migration function
func Execute(q query.Query, oldDs datastore.Batching, newDS datastore.Batching, oldType reflect.Type, migrateFunc reflect.Value) ([]datastore.Key, error) {
	qres, err := oldDs.Query(q)
	if err != nil {
		return nil, err
	}
	defer qres.Close()

	batch, err := newDS.Batch()
	if err != nil {
		return nil, fmt.Errorf("batch error: %w", err)
	}

	keys, errs := execute(qres, oldDs, newDS, oldType, migrateFunc, batch)
	err = batch.Commit()
	if err != nil {
		return nil, fmt.Errorf("committing: %w", err)
	}

	return keys, errs
}

func execute(qres query.Results, oldDs, newDS datastore.Batching, oldType reflect.Type, migrateFunc reflect.Value, batch datastore.Batch) (keys []datastore.Key, errs error) {

	for res := range qres.Next() {
		if res.Error != nil {
			errs = res.Error
			return
		}

		oldElem := reflect.New(oldType.Elem())
		err := cborutil.ReadCborRPC(bytes.NewReader(res.Value), oldElem.Interface())
		if err != nil {
			errs = multierr.Append(errs, fmt.Errorf("decoding state for key '%s': %w", res.Key, err))
			continue
		}

		outputs := migrateFunc.Call([]reflect.Value{oldElem})
		err, ok := outputs[1].Interface().(error)
		if ok && err != nil {
			errs = multierr.Append(errs, fmt.Errorf("attempting to transform to new state '%s': %w", res.Key, err))
			continue
		}
		has, err := newDS.Has(datastore.NewKey(res.Key))
		if err != nil {
			errs = err
			return
		}
		if has {
			errs = multierr.Append(errs, fmt.Errorf("already tracking state in new db for '%s'", res.Key))
			continue
		}
		bts, err := cborutil.Dump(outputs[0].Interface().(cbg.CBORMarshaler))
		if err != nil {
			errs = multierr.Append(errs, fmt.Errorf("encoding state for key '%s': %w", res.Key, err))
			continue
		}
		err = batch.Put(datastore.NewKey(res.Key), bts)
		if err != nil {
			errs = err
			return
		}
		keys = append(keys, datastore.NewKey(res.Key))
	}
	return
}

var versioningKey = datastore.NewKey("/versions/current")

// To attempts to migrate the database to the target version, reading from current version from the predefined key
// and applying migrations as need to reach the target version
// it returns the final database version (ideally = target) and any errors encountered
func To(ds datastore.Batching, migrations versioning.VersionedMigrationList, to versioning.VersionKey) (versioning.VersionKey, error) {
	sort.Sort(migrations)
	if !verifyIntegrity(migrations) {
		return versioning.VersionKey(""), fmt.Errorf("migrations list must be contiguous")
	}
	verBytes, err := ds.Get(versioningKey)
	if err == datastore.ErrNotFound {
		hasData, err := notEmpty(ds)
		if err != nil {
			return versioning.VersionKey(""), fmt.Errorf("determining if store has data: %w", err)
		}
		if hasData {
			if migrations[0].OldVersion() != versioning.VersionKey("") {
				return versioning.VersionKey(""), errors.New("cannot migrate from an unversioned database")
			}
			verBytes = []byte("")
		} else {
			// empty database -- we'll treat it as ready to go after writing current version
			err = ds.Put(versioningKey, []byte(to))
			if err != nil {
				return versioning.VersionKey(""), fmt.Errorf("writing version: %w", err)
			}
			return to, nil
		}
	} else if err != nil {
		return versioning.VersionKey(""), fmt.Errorf("reading version: %w", err)
	}

	currentVersion := versioning.VersionKey(verBytes)
	final, err := runMigrations(ds, migrations, currentVersion, to)
	ferr := ds.Put(versioningKey, []byte(final))
	if err != nil {
		return final, err
	}
	return final, ferr
}

func runMigrations(ds datastore.Batching, migrations versioning.VersionedMigrationList, current versioning.VersionKey, target versioning.VersionKey) (versioning.VersionKey, error) {
	if target > current {
		for _, migration := range migrations {
			if migration.OldVersion() == current {
				keys, err := migration.Up(ds)
				if err != nil {
					versionedKeys := utils.KeysForVersion(migration.NewVersion(), keys)
					_ = deleteKeys(ds, versionedKeys)
					return current, fmt.Errorf("running up migration: %w", err)
				}
				current = migration.NewVersion()
				versionedKeys := utils.KeysForVersion(migration.OldVersion(), keys)
				err = deleteKeys(ds, versionedKeys)
				if err != nil {
					return current, fmt.Errorf("deleting keys: %w", err)
				}
				if current == target {
					return current, nil
				}
			}
		}
	} else if target < current {
		sort.Sort(sort.Reverse(migrations))
		for _, migration := range migrations {
			reversible, ok := migration.(versioning.ReversibleVersionedMigration)
			if ok && reversible.NewVersion() == current {
				keys, err := reversible.Down(ds)
				if err != nil {
					versionedKeys := utils.KeysForVersion(migration.OldVersion(), keys)
					_ = deleteKeys(ds, versionedKeys)
					return current, fmt.Errorf("running down migration: %w", err)
				}
				current = migration.OldVersion()
				versionedKeys := utils.KeysForVersion(migration.NewVersion(), keys)
				err = deleteKeys(ds, versionedKeys)
				if err != nil {
					return current, fmt.Errorf("deleting keys: %w", err)
				}
				if current == target {
					return current, nil
				}
			}
		}
	} else if target == current {
		return current, nil
	}
	return current, errors.New("never reached target database version")
}

func notEmpty(ds datastore.Batching) (bool, error) {
	qres, err := ds.Query(query.Query{})
	if err != nil {
		return false, err
	}
	_, hasData := qres.NextSync()
	err = qres.Close()
	return hasData, err
}

func deleteKeys(ds datastore.Batching, keys []datastore.Key) error {
	batch, err := ds.Batch()
	if err != nil {
		return fmt.Errorf("batch error: %w", err)
	}
	for _, key := range keys {
		err = batch.Delete(key)
		if err != nil {
			_ = batch.Commit()
			return err
		}
	}

	err = batch.Commit()
	if err != nil {
		return fmt.Errorf("committing: %w", err)
	}
	return nil
}

// verify integrity verifies every version in a migration lists migrations to the old
// version for the next migration
func verifyIntegrity(migrations versioning.VersionedMigrationList) bool {
	for i := 0; i < migrations.Len()-1; i++ {
		if migrations[i].NewVersion() != migrations[i+1].OldVersion() {
			return false
		}
	}
	return true
}
