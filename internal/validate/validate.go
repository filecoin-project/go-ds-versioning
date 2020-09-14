package validate

import (
	"errors"
	"reflect"

	versioning "github.com/filecoin-project/go-ds-versioning/pkg"
	cbg "github.com/whyrusleeping/cbor-gen"
)

// CheckMigration validationes that a migration func matches the required signature for
// this kind of function
func CheckMigration(migrate versioning.MigrationFunc) (reflect.Type, reflect.Type, error) {
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
