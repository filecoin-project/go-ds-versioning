package versioning_test

import (
	"bytes"
	"errors"
	"reflect"
	"testing"

	cborutil "github.com/filecoin-project/go-cbor-util"
	versioning "github.com/filecoin-project/go-ds-versioning"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	"github.com/stretchr/testify/require"
	cbg "github.com/whyrusleeping/cbor-gen"
	"go.uber.org/multierr"
)

func TestValidateMigrationFunc(t *testing.T) {
	testCases := map[string]struct {
		migrateFunc        versioning.MigrationFunc
		expectedErr        error
		expectedInputType  reflect.Type
		expectedOutputType reflect.Type
	}{
		"not given a function": {
			migrateFunc: 8,
			expectedErr: errors.New("migration must be a function"),
		},
		"given a function that takes the wrong number of arguments": {
			migrateFunc: func() {},
			expectedErr: errors.New("migration must take exactly one argument"),
		},
		"given a function that produces the wrong number of outputs": {
			migrateFunc: func(c *cbg.CborInt) *cbg.CborBool {
				var out cbg.CborBool
				if *c != 0 {
					out = true
				}
				return &out
			},
			expectedErr: errors.New("migration must produce exactly two return values"),
		},
		"given a function that takes an input that isn't a cbor unmarshaller": {
			migrateFunc: func(c *uint64) (*cbg.CborBool, error) {
				var out cbg.CborBool
				if *c != 0 {
					out = true
				}
				return &out, nil
			},
			expectedErr: errors.New("input must be an unmarshallable CBOR struct"),
		},
		"given a function that produces an output that isn't a cbor marshaller": {
			migrateFunc: func(c *cbg.CborInt) (*bool, error) {
				var out cbg.CborBool
				if *c != 0 {
					out = true
				}
				return (*bool)(&out), nil
			},
			expectedErr: errors.New("first output must be an marshallable CBOR struct"),
		},
		"given a function that produces a second output that isn't an error": {
			migrateFunc: func(c *cbg.CborInt) (*cbg.CborBool, int) {
				var out cbg.CborBool
				if *c != 0 {
					out = true
				}
				return &out, 0
			},
			expectedErr: errors.New("second output must be an error interface"),
		},
		"given a function that matches the required format": {
			migrateFunc: func(c *cbg.CborInt) (*cbg.CborBool, error) {
				var out cbg.CborBool
				if *c != 0 {
					out = true
				}
				return &out, nil
			},
			expectedErr:        nil,
			expectedInputType:  reflect.TypeOf(new(cbg.CborInt)),
			expectedOutputType: reflect.TypeOf(new(cbg.CborBool)),
		},
	}
	for testCase, data := range testCases {
		t.Run(testCase, func(t *testing.T) {
			inputType, outputType, err := versioning.ValidateMigrationFunc(data.migrateFunc)
			if data.expectedErr == nil {
				require.NoError(t, err)
				require.Equal(t, data.expectedInputType, inputType)
				require.Equal(t, data.expectedOutputType, outputType)
			} else {
				require.EqualError(t, err, data.expectedErr.Error())
			}
		})
	}
}

func TestMigrateDatabase(t *testing.T) {
	var appleCount = cbg.CborInt(30)
	var orangeCount = cbg.CborInt(0)
	var untransformableCount = cbg.CborInt(42)
	var origBool = cbg.CborBool(true)
	testCases := map[string]struct {
		inputDatabase          map[string]cbg.CBORMarshaler
		expectedOutputDatabase map[string]cbg.CborBool
		preloadOutputs         map[string]cbg.CborBool
		expectedErrs           []error
	}{
		"it works": {
			inputDatabase: map[string]cbg.CBORMarshaler{
				"/apples":  &appleCount,
				"/oranges": &orangeCount,
			},
			expectedOutputDatabase: map[string]cbg.CborBool{
				"/apples":  true,
				"/oranges": false,
			},
		},
		"issue unmarshalling old values": {
			inputDatabase: map[string]cbg.CBORMarshaler{
				"/apples":      &appleCount,
				"/oranges":     &orangeCount,
				"/anotherkind": &origBool,
			},
			expectedOutputDatabase: map[string]cbg.CborBool{
				"/apples":  true,
				"/oranges": false,
			},
			expectedErrs: []error{errors.New("decoding state for key '/anotherkind': wrong type for int64 field: 7")},
		},
		"issue transforming a value": {
			inputDatabase: map[string]cbg.CBORMarshaler{
				"/apples":          &appleCount,
				"/oranges":         &orangeCount,
				"/untransformable": &untransformableCount,
			},
			expectedOutputDatabase: map[string]cbg.CborBool{
				"/apples":  true,
				"/oranges": false,
			},
			expectedErrs: []error{errors.New("attempting to transform to new state '/untransformable': the meaning of life is untransformable")},
		},
		"value already tracked in DS": {
			inputDatabase: map[string]cbg.CBORMarshaler{
				"/apples":  &appleCount,
				"/oranges": &orangeCount,
			},
			expectedOutputDatabase: map[string]cbg.CborBool{
				"/apples":  false,
				"/oranges": false,
			},
			preloadOutputs: map[string]cbg.CborBool{
				"/apples": false,
			},
			expectedErrs: []error{errors.New("already tracking state in new db for '/apples'")},
		},
	}
	transform := func(c *cbg.CborInt) (*cbg.CborBool, error) {
		var out cbg.CborBool
		if *c != 0 {
			out = true
		}
		if *c == 42 {
			return nil, errors.New("the meaning of life is untransformable")
		}
		return &out, nil
	}
	transformValue := reflect.ValueOf(transform)
	oldType := reflect.TypeOf(new(cbg.CborInt))
	for testCase, data := range testCases {
		t.Run(testCase, func(t *testing.T) {
			ds1 := datastore.NewMapDatastore()
			ds2 := datastore.NewMapDatastore()
			if data.inputDatabase != nil {
				for key, value := range data.inputDatabase {
					buf := new(bytes.Buffer)
					err := value.MarshalCBOR(buf)
					require.NoError(t, err)
					err = ds1.Put(datastore.NewKey(key), buf.Bytes())
					require.NoError(t, err)
				}
			}
			if data.preloadOutputs != nil {
				for key, value := range data.preloadOutputs {
					buf := new(bytes.Buffer)
					err := value.MarshalCBOR(buf)
					require.NoError(t, err)
					err = ds2.Put(datastore.NewKey(key), buf.Bytes())
					require.NoError(t, err)
				}
			}
			err := versioning.MigrateDatabase(query.Query{}, ds1, ds2, oldType, transformValue)
			errs := multierr.Errors(err)
			require.Equal(t, len(data.expectedErrs), len(errs))
			for i, err := range errs {
				require.EqualError(t, err, data.expectedErrs[i].Error())
			}
			outputDatabase := make(map[string]cbg.CborBool)
			res, err := ds2.Query(query.Query{})
			require.NoError(t, err)
			defer res.Close()
			for {
				res, ok := res.NextSync()
				if !ok {
					break
				}
				require.NoError(t, res.Error)
				var out cbg.CborBool
				err := cborutil.ReadCborRPC(bytes.NewReader(res.Value), &out)
				require.NoError(t, err)
				outputDatabase[res.Key] = out
			}
			require.Equal(t, data.expectedOutputDatabase, outputDatabase)
		})
	}

}
