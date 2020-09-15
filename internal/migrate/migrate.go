package migrate

import (
	"bytes"
	"fmt"
	"reflect"

	cborutil "github.com/filecoin-project/go-cbor-util"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	cbg "github.com/whyrusleeping/cbor-gen"
	"go.uber.org/multierr"
)

// ExecuteMigration executes a database migration from datastore to another, using the given migration function
func ExecuteMigration(q query.Query, oldDs datastore.Batching, newDS datastore.Batching, oldType reflect.Type, migrateFunc reflect.Value) ([]datastore.Key, error) {
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
