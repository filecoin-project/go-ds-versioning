package migrate

import (
	"bytes"
	"reflect"

	cborutil "github.com/filecoin-project/go-cbor-util"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	cbg "github.com/whyrusleeping/cbor-gen"
	"go.uber.org/multierr"
	"golang.org/x/xerrors"
)

// ExecuteMigration executes a database migration from datastore to another, using the given migration function
func ExecuteMigration(q query.Query, oldDs datastore.Batching, newDS datastore.Batching, oldType reflect.Type, migrateFunc reflect.Value) error {
	res, err := oldDs.Query(q)
	if err != nil {
		return err
	}
	defer res.Close()

	var errs error

	for {
		res, ok := res.NextSync()
		if !ok {
			break
		}
		if res.Error != nil {
			return res.Error
		}

		oldElem := reflect.New(oldType.Elem())
		err := cborutil.ReadCborRPC(bytes.NewReader(res.Value), oldElem.Interface())
		if err != nil {
			errs = multierr.Append(errs, xerrors.Errorf("decoding state for key '%s': %w", res.Key, err))
			continue
		}

		outputs := migrateFunc.Call([]reflect.Value{oldElem})
		err, ok = outputs[1].Interface().(error)
		if ok && err != nil {
			errs = multierr.Append(errs, xerrors.Errorf("attempting to transform to new state '%s': %w", res.Key, err))
			continue
		}
		has, err := newDS.Has(datastore.NewKey(res.Key))
		if err != nil {
			return err
		}
		if has {
			errs = multierr.Append(errs, xerrors.Errorf("already tracking state in new db for '%s'", res.Key))
			continue
		}
		b, err := cborutil.Dump(outputs[0].Interface().(cbg.CBORMarshaler))
		if err != nil {
			errs = multierr.Append(errs, xerrors.Errorf("encoding state for key '%s': %w", res.Key, err))
			continue
		}
		err = newDS.Put(datastore.NewKey(res.Key), b)
		if err != nil {
			return err
		}
	}

	return errs
}
