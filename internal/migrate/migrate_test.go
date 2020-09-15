package migrate_test

import (
	"bytes"
	"errors"
	"reflect"
	"testing"

	cborutil "github.com/filecoin-project/go-cbor-util"
	"github.com/filecoin-project/go-ds-versioning/internal/migrate"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	"github.com/stretchr/testify/require"
	cbg "github.com/whyrusleeping/cbor-gen"
	"go.uber.org/multierr"
	"go4.org/sort"
)

func TestExecuteMigration(t *testing.T) {
	var appleCount = cbg.CborInt(30)
	var orangeCount = cbg.CborInt(0)
	var untransformableCount = cbg.CborInt(42)
	var origBool = cbg.CborBool(true)
	testCases := map[string]struct {
		inputDatabase          map[string]cbg.CBORMarshaler
		expectedOutputDatabase map[string]cbg.CborBool
		preloadOutputs         map[string]cbg.CborBool
		expectedKeys           []datastore.Key
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
			expectedKeys: []datastore.Key{datastore.NewKey("/apples"), datastore.NewKey("/oranges")},
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
			expectedKeys: []datastore.Key{datastore.NewKey("/apples"), datastore.NewKey("/oranges")},
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
			expectedKeys: []datastore.Key{datastore.NewKey("/apples"), datastore.NewKey("/oranges")},
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
			expectedKeys: []datastore.Key{datastore.NewKey("/oranges")},
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
			migrated, err := migrate.ExecuteMigration(query.Query{}, ds1, ds2, oldType, transformValue)
			errs := multierr.Errors(err)
			require.Equal(t, len(data.expectedErrs), len(errs))
			for i, err := range errs {
				require.EqualError(t, err, data.expectedErrs[i].Error())
			}
			sort.Slice(migrated, func(i, j int) bool { return migrated[i].String() < migrated[j].String() })
			require.Equal(t, data.expectedKeys, migrated)
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
