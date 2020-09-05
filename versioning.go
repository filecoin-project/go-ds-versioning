package versioning

import (
	"bytes"
	"errors"
	"reflect"

	cborutil "github.com/filecoin-project/go-cbor-util"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	cbg "github.com/whyrusleeping/cbor-gen"
	"go.uber.org/multierr"
	"golang.org/x/xerrors"
)

// MigrationFunc is a function to transform an single element of one type of data into
// a single element of another type of data. It has the following form:
// func<T extends cbg.CBORUnmarshaller, U extends cbg.CBORMarshaller>(old T) (new U, error)
type MigrationFunc interface{}

// DatastoreMigration can run a migration of a datastore that is a table
// of one kind of structured data and write it to a table that is another kind of
// structured data
type DatastoreMigration interface {
	Up(oldDs datastore.Batching, newDS datastore.Batching) error
}

// ReversableDatastoreMigration is
type ReversableDatastoreMigration interface {
	DatastoreMigration
	Down(newDs datastore.Batching, oldDS datastore.Batching) error
}

func MigrateDatabase(q query.Query, oldDs datastore.Batching, newDS datastore.Batching, oldType reflect.Type, migrateFunc reflect.Value) error {
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

func NewDatastoreMigration(up MigrationFunc) (DatastoreMigration, error) {
	return nil, nil
}

func NewReversableDatastoreMigration(up MigrationFunc, down MigrationFunc) (ReversableDatastoreMigration, error) {
	return nil, nil
}

// ValidateMigrationFunc validationes that a migration func matches the required signature for
// this kind of function
func ValidateMigrationFunc(migrate MigrationFunc) (reflect.Type, reflect.Type, error) {
	migrateType := reflect.TypeOf(migrate)
	if migrateType.Kind() != reflect.Func {
		return nil, nil, errors.New("migration must be a function")
	}
	if migrateType.NumIn() != 1 {
		return nil, nil, errors.New("migration must take exactly one argument")
	}
	if migrateType.NumOut() != 2 {
		return nil, nil, errors.New("migration must produce exactly two return values")
	}
	input := migrateType.In(0)
	if !input.Implements(reflect.TypeOf((*cbg.CBORUnmarshaler)(nil)).Elem()) {
		return nil, nil, errors.New("input must be an unmarshallable CBOR struct")
	}
	output := migrateType.Out(0)
	if !output.Implements(reflect.TypeOf((*cbg.CBORMarshaler)(nil)).Elem()) {
		return nil, nil, errors.New("first output must be an marshallable CBOR struct")
	}
	errOutValue := reflect.New(migrateType.Out(1))
	if _, ok := errOutValue.Interface().(*error); !ok {
		return nil, nil, errors.New("second output must be an error interface")
	}
	return input, output, nil
}
