package datastore_test

import (
	"bytes"
	"testing"

	datastore "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	"github.com/stretchr/testify/require"
	cbg "github.com/whyrusleeping/cbor-gen"

	versioning "github.com/filecoin-project/go-ds-versioning/pkg"
	versioned "github.com/filecoin-project/go-ds-versioning/pkg/datastore"
)

func TestDatastore(t *testing.T) {
	testCases := map[string]struct {
		migrationErr  error
		inputDatabase map[string]cbg.CBORMarshaler
		test          func(t *testing.T, ds datastore.Batching)
	}{
		"Get, not ready": {
			migrationErr: versioning.ErrMigrationsNotRun,
			inputDatabase: map[string]cbg.CBORMarshaler{
				"/apples": newInt(54),
			},
			test: func(t *testing.T, ds datastore.Batching) {
				val, err := ds.Get(datastore.NewKey("/apples"))
				require.Nil(t, val)
				require.EqualError(t, err, versioning.ErrMigrationsNotRun.Error())
			},
		},
		"Get, ready": {
			inputDatabase: map[string]cbg.CBORMarshaler{
				"/apples": newInt(54),
			},
			test: func(t *testing.T, ds datastore.Batching) {
				val, err := ds.Get(datastore.NewKey("/apples"))
				checkInt(t, val, 54)
				require.NoError(t, err)
			},
		},
		"Put, not ready": {
			migrationErr: versioning.ErrMigrationsNotRun,
			test: func(t *testing.T, ds datastore.Batching) {
				err := ds.Put(datastore.NewKey("/apples"), toBytes(t, newInt(54)))
				require.EqualError(t, err, versioning.ErrMigrationsNotRun.Error())
			},
		},
		"Put, ready": {
			test: func(t *testing.T, ds datastore.Batching) {
				err := ds.Put(datastore.NewKey("/apples"), toBytes(t, newInt(54)))
				require.NoError(t, err)
				val, err := ds.Get(datastore.NewKey("/apples"))
				checkInt(t, val, 54)
				require.NoError(t, err)
			},
		},
		"Has, not ready": {
			migrationErr: versioning.ErrMigrationsNotRun,
			inputDatabase: map[string]cbg.CBORMarshaler{
				"/apples": newInt(54),
			},
			test: func(t *testing.T, ds datastore.Batching) {
				has, err := ds.Has(datastore.NewKey("/apples"))
				require.False(t, has)
				require.EqualError(t, err, versioning.ErrMigrationsNotRun.Error())
			},
		},
		"Has, ready": {
			inputDatabase: map[string]cbg.CBORMarshaler{
				"/apples": newInt(54),
			},
			test: func(t *testing.T, ds datastore.Batching) {
				has, err := ds.Has(datastore.NewKey("/apples"))
				require.True(t, has)
				require.NoError(t, err)
			},
		},
		"GetSize, not ready": {
			migrationErr: versioning.ErrMigrationsNotRun,
			inputDatabase: map[string]cbg.CBORMarshaler{
				"/apples": newInt(54),
			},
			test: func(t *testing.T, ds datastore.Batching) {
				size, err := ds.GetSize(datastore.NewKey("/apples"))
				require.Zero(t, size)
				require.EqualError(t, err, versioning.ErrMigrationsNotRun.Error())
			},
		},
		"GetSize, ready": {
			inputDatabase: map[string]cbg.CBORMarshaler{
				"/apples": newInt(54),
			},
			test: func(t *testing.T, ds datastore.Batching) {
				size, err := ds.GetSize(datastore.NewKey("/apples"))
				require.Equal(t, len(toBytes(t, newInt(54))), size)
				require.NoError(t, err)
			},
		},
		"Query, not ready": {
			migrationErr: versioning.ErrMigrationsNotRun,
			inputDatabase: map[string]cbg.CBORMarshaler{
				"/apples": newInt(54),
			},
			test: func(t *testing.T, ds datastore.Batching) {
				results, err := ds.Query(query.Query{})
				require.Nil(t, results)
				require.EqualError(t, err, versioning.ErrMigrationsNotRun.Error())
			},
		},
		"Query, ready": {
			inputDatabase: map[string]cbg.CBORMarshaler{
				"/apples": newInt(54),
			},
			test: func(t *testing.T, ds datastore.Batching) {
				results, err := ds.Query(query.Query{})
				require.NoError(t, err)
				rest, err := results.Rest()
				require.NoError(t, err)
				require.Equal(t, 1, len(rest))
			},
		},
		"Close, not ready": {
			migrationErr: versioning.ErrMigrationsNotRun,
			test: func(t *testing.T, ds datastore.Batching) {
				err := ds.Close()
				require.EqualError(t, err, versioning.ErrMigrationsNotRun.Error())
			},
		},
		"Close, ready": {
			test: func(t *testing.T, ds datastore.Batching) {
				err := ds.Close()
				require.NoError(t, err)
			},
		},
		"Batch, not ready": {
			migrationErr: versioning.ErrMigrationsNotRun,
			test: func(t *testing.T, ds datastore.Batching) {
				batch, err := ds.Batch()
				require.Nil(t, batch)
				require.EqualError(t, err, versioning.ErrMigrationsNotRun.Error())
			},
		},
		"Batch, ready": {
			test: func(t *testing.T, ds datastore.Batching) {
				batch, err := ds.Batch()
				require.NotNil(t, batch)
				require.NoError(t, err)
			},
		},
	}
	for testCase, data := range testCases {
		t.Run(testCase, func(t *testing.T) {
			ds := datastore.NewMapDatastore()
			if data.inputDatabase != nil {
				for key, value := range data.inputDatabase {
					buf := new(bytes.Buffer)
					err := value.MarshalCBOR(buf)
					require.NoError(t, err)
					err = ds.Put(datastore.NewKey(key), buf.Bytes())
					require.NoError(t, err)
				}
			}
			ms := migrationState{data.migrationErr}
			migratedDs := versioned.NewMigratedDatastore(ds, ms)
			data.test(t, migratedDs)
		})
	}
}

type migrationState struct {
	err error
}

func (ms migrationState) ReadyError() error {
	return ms.err
}

func newInt(i int64) *cbg.CborInt {
	val := cbg.CborInt(i)
	return &val
}

func checkInt(t *testing.T, raw []byte, i int64) {
	var val cbg.CborInt
	err := val.UnmarshalCBOR(bytes.NewReader(raw))
	require.NoError(t, err)
	require.Equal(t, cbg.CborInt(i), val)
}

func toBytes(t *testing.T, val cbg.CBORMarshaler) []byte {
	buf := new(bytes.Buffer)
	err := val.MarshalCBOR(buf)
	require.NoError(t, err)
	return buf.Bytes()
}
