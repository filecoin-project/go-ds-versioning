package migrate_test

import (
	"bytes"
	"errors"
	"reflect"
	"testing"

	cborutil "github.com/filecoin-project/go-cbor-util"
	"github.com/filecoin-project/go-ds-versioning/internal/migrate"
	versioning "github.com/filecoin-project/go-ds-versioning/pkg"
	"github.com/filecoin-project/go-ds-versioning/pkg/versioned"
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
			migrated, err := migrate.Execute(query.Query{}, ds1, ds2, oldType, transformValue)
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

func TestTo(t *testing.T) {
	addMigration := func(c *cbg.CborInt) (*cbg.CborInt, error) {
		newCount := *c + 7
		return &newCount, nil
	}
	subMigration := func(c *cbg.CborInt) (*cbg.CborInt, error) {
		newCount := *c - 7
		return &newCount, nil
	}
	multiplyMigration := func(c *cbg.CborInt) (*cbg.CborInt, error) {
		newCount := *c * 4
		return &newCount, nil
	}
	divideMigration := func(c *cbg.CborInt) (*cbg.CborInt, error) {
		newCount := *c / 4
		return &newCount, nil
	}
	errorMigration := func(c *cbg.CborInt) (*cbg.CborInt, error) {
		if *c == 10 {
			return nil, errors.New("could not migrate")
		}
		newCount := *c + 5
		return &newCount, nil
	}

	testCases := map[string]struct {
		inputDatabase          map[string][]byte
		expectedOutputDatabase map[string][]byte
		migrationBuilders      versioned.BuilderList
		target                 versioning.VersionKey
		expectedFinalVersion   versioning.VersionKey
		expectedErr            error
	}{
		"empty database": {
			inputDatabase: map[string][]byte{},
			expectedOutputDatabase: map[string][]byte{
				"/versions/current": versionData("2"),
			},
			target:               "2",
			expectedFinalVersion: "2",
			migrationBuilders: versioned.BuilderList{
				versioned.NewVersionedBuilder(addMigration, "1").Reversible(subMigration),
				versioned.NewVersionedBuilder(multiplyMigration, "2").Reversible(divideMigration).OldVersion("1"),
			},
		},
		"unversioned database with data": {
			inputDatabase: map[string][]byte{
				"/apples":  numData(t, 7),
				"/oranges": numData(t, 3),
			},
			expectedOutputDatabase: map[string][]byte{
				"/versions/current": versionData("2"),
				"/2/apples":         numData(t, 56),
				"/2/oranges":        numData(t, 40),
			},
			target:               "2",
			expectedFinalVersion: "2",
			migrationBuilders: versioned.BuilderList{
				versioned.NewVersionedBuilder(addMigration, "1").Reversible(subMigration),
				versioned.NewVersionedBuilder(multiplyMigration, "2").Reversible(divideMigration).OldVersion("1"),
			},
		},
		"unversioned database with data, no initial migration": {
			inputDatabase: map[string][]byte{
				"/apples":  numData(t, 7),
				"/oranges": numData(t, 3),
			},
			expectedOutputDatabase: map[string][]byte{
				"/apples":  numData(t, 7),
				"/oranges": numData(t, 3),
			},
			target:               "2",
			expectedFinalVersion: "",
			expectedErr:          errors.New("cannot migrate from an unversioned database"),
			migrationBuilders: versioned.BuilderList{
				versioned.NewVersionedBuilder(multiplyMigration, "2").Reversible(divideMigration).OldVersion("1"),
			},
		},
		"discontigous migration list": {
			inputDatabase:          map[string][]byte{},
			expectedOutputDatabase: map[string][]byte{},
			target:                 "2",
			expectedFinalVersion:   "",
			expectedErr:            errors.New("migrations list must be contiguous"),
			migrationBuilders: versioned.BuilderList{
				versioned.NewVersionedBuilder(addMigration, "1").Reversible(subMigration),
				versioned.NewVersionedBuilder(multiplyMigration, "3").Reversible(divideMigration).OldVersion("2"),
			},
		},
		"normal migration": {
			inputDatabase: map[string][]byte{
				"/versions/current": versionData("1"),
				"/1/apples":         numData(t, 14),
				"/1/oranges":        numData(t, 10),
			},
			expectedOutputDatabase: map[string][]byte{
				"/versions/current": versionData("2"),
				"/2/apples":         numData(t, 56),
				"/2/oranges":        numData(t, 40),
			},
			target:               "2",
			expectedFinalVersion: "2",
			migrationBuilders: versioned.BuilderList{
				versioned.NewVersionedBuilder(multiplyMigration, "2").Reversible(divideMigration).OldVersion("1"),
			},
		},
		"incomplete migration": {
			inputDatabase: map[string][]byte{
				"/versions/current": versionData("1"),
				"/1/apples":         numData(t, 14),
				"/1/oranges":        numData(t, 10),
			},
			expectedOutputDatabase: map[string][]byte{
				"/versions/current": versionData("2"),
				"/2/apples":         numData(t, 56),
				"/2/oranges":        numData(t, 40),
			},
			target:               "3",
			expectedErr:          errors.New("never reached target database version"),
			expectedFinalVersion: "2",
			migrationBuilders: versioned.BuilderList{
				versioned.NewVersionedBuilder(multiplyMigration, "2").Reversible(divideMigration).OldVersion("1"),
			},
		},
		"migrate down": {
			inputDatabase: map[string][]byte{
				"/versions/current": versionData("3"),
				"/3/apples":         numData(t, 56),
				"/3/oranges":        numData(t, 40),
			},
			expectedOutputDatabase: map[string][]byte{
				"/versions/current": versionData("1"),
				"/1/apples":         numData(t, 7),
				"/1/oranges":        numData(t, 3),
			},
			target:               "1",
			expectedFinalVersion: "1",
			migrationBuilders: versioned.BuilderList{
				versioned.NewVersionedBuilder(addMigration, "2").Reversible(subMigration).OldVersion("1"),
				versioned.NewVersionedBuilder(multiplyMigration, "3").Reversible(divideMigration).OldVersion("2"),
			},
		},
		"error while migrating, first migration": {
			inputDatabase: map[string][]byte{
				"/versions/current": versionData("1"),
				"/1/apples":         numData(t, 14),
				"/1/oranges":        numData(t, 10),
			},
			expectedOutputDatabase: map[string][]byte{
				"/versions/current": versionData("1"),
				"/1/apples":         numData(t, 14),
				"/1/oranges":        numData(t, 10),
			},
			target:               "2",
			expectedFinalVersion: "1",
			expectedErr:          errors.New("running up migration: attempting to transform to new state '/oranges': could not migrate"),
			migrationBuilders: versioned.BuilderList{
				versioned.NewVersionedBuilder(errorMigration, "2").OldVersion("1"),
			},
		},
		"error while migrating, second migration": {
			inputDatabase: map[string][]byte{
				"/versions/current": versionData("1"),
				"/1/apples":         numData(t, 7),
				"/1/oranges":        numData(t, 3),
			},
			expectedOutputDatabase: map[string][]byte{
				"/versions/current": versionData("2"),
				"/2/apples":         numData(t, 14),
				"/2/oranges":        numData(t, 10),
			},
			target:               "3",
			expectedFinalVersion: "2",
			expectedErr:          errors.New("running up migration: attempting to transform to new state '/oranges': could not migrate"),
			migrationBuilders: versioned.BuilderList{
				versioned.NewVersionedBuilder(addMigration, "2").OldVersion("1"),
				versioned.NewVersionedBuilder(errorMigration, "3").OldVersion("2"),
			},
		},
	}
	for testCase, data := range testCases {
		t.Run(testCase, func(t *testing.T) {
			ds1 := datastore.NewMapDatastore()
			if data.inputDatabase != nil {
				for key, value := range data.inputDatabase {
					err := ds1.Put(datastore.NewKey(key), value)
					require.NoError(t, err)
				}
			}
			migrations, err := data.migrationBuilders.Build()
			require.NoError(t, err)
			finalVersion, err := migrate.To(ds1, migrations, data.target)
			require.Equal(t, data.expectedFinalVersion, finalVersion)
			if data.expectedErr == nil {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, data.expectedErr.Error())
			}
			outputDatabase := make(map[string][]byte)
			res, err := ds1.Query(query.Query{})
			require.NoError(t, err)
			defer res.Close()
			for {
				res, ok := res.NextSync()
				if !ok {
					break
				}
				require.NoError(t, res.Error)
				require.NoError(t, err)
				outputDatabase[res.Key] = res.Value
			}
			require.Equal(t, data.expectedOutputDatabase, outputDatabase)
		})
	}
}

func versionData(versionKey versioning.VersionKey) []byte {
	return []byte(versionKey)
}

func numData(t *testing.T, num int64) []byte {
	buf := new(bytes.Buffer)
	var value cbg.CborInt = cbg.CborInt(num)
	err := value.MarshalCBOR(buf)
	require.NoError(t, err)
	return buf.Bytes()
}
