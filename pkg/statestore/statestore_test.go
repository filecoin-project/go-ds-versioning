package statestore_test

import (
	"fmt"
	"testing"

	versioning "github.com/filecoin-project/go-ds-versioning/pkg"
	versioned "github.com/filecoin-project/go-ds-versioning/pkg/statestore"
	"github.com/filecoin-project/go-statestore"
	"github.com/ipfs/go-datastore"
	"github.com/stretchr/testify/require"
	cbg "github.com/whyrusleeping/cbor-gen"
)

func TestStateStore(t *testing.T) {
	testCases := map[string]struct {
		migrationErr  error
		inputDatabase map[fmt.Stringer]cbg.CBORMarshaler
		test          func(t *testing.T, stateStore versioned.StateStore)
	}{
		"Get, not ready": {
			migrationErr: versioning.ErrMigrationsNotRun,
			inputDatabase: map[fmt.Stringer]cbg.CBORMarshaler{
				stringer("/apples"): newInt(54),
			},
			test: func(t *testing.T, ss versioned.StateStore) {
				storedState := ss.Get(stringer("/apples"))
				var out cbg.CborInt
				require.EqualError(t, storedState.Get(&out), versioning.ErrMigrationsNotRun.Error())
			},
		},
		"Get, ready": {
			inputDatabase: map[fmt.Stringer]cbg.CBORMarshaler{
				stringer("/apples"): newInt(54),
			},
			test: func(t *testing.T, ss versioned.StateStore) {
				storedState := ss.Get(stringer("/apples"))
				var out cbg.CborInt
				err := storedState.Get(&out)
				require.Equal(t, cbg.CborInt(54), out)
				require.NoError(t, err)
			},
		},
		"Begin, not ready": {
			migrationErr: versioning.ErrMigrationsNotRun,
			test: func(t *testing.T, ss versioned.StateStore) {
				require.EqualError(t, ss.Begin(stringer("/apples"), newInt(54)), versioning.ErrMigrationsNotRun.Error())
			},
		},
		"Beging, ready": {
			test: func(t *testing.T, ss versioned.StateStore) {
				err := ss.Begin(stringer("/apples"), newInt(54))
				require.NoError(t, err)
				storedState := ss.Get(stringer("/apples"))
				var out cbg.CborInt
				err = storedState.Get(&out)
				require.Equal(t, cbg.CborInt(54), out)
				require.NoError(t, err)
			},
		},
		"List, not ready": {
			migrationErr: versioning.ErrMigrationsNotRun,
			inputDatabase: map[fmt.Stringer]cbg.CBORMarshaler{
				stringer("/apples"): newInt(54),
			},
			test: func(t *testing.T, ss versioned.StateStore) {
				var out []*cbg.CborInt
				require.EqualError(t, ss.List(&out), versioning.ErrMigrationsNotRun.Error())
			},
		},
		"List, ready": {
			inputDatabase: map[fmt.Stringer]cbg.CBORMarshaler{
				stringer("/apples"): newInt(54),
			},
			test: func(t *testing.T, ss versioned.StateStore) {
				var out []cbg.CborInt
				err := ss.List(&out)
				require.Len(t, out, 1)
				require.Equal(t, cbg.CborInt(54), out[0])
				require.NoError(t, err)
			},
		},
		"Has, not ready": {
			migrationErr: versioning.ErrMigrationsNotRun,
			inputDatabase: map[fmt.Stringer]cbg.CBORMarshaler{
				stringer("/apples"): newInt(54),
			},
			test: func(t *testing.T, ss versioned.StateStore) {
				has, err := ss.Has(stringer("/apples"))
				require.False(t, has)
				require.EqualError(t, err, versioning.ErrMigrationsNotRun.Error())
			},
		},
		"Has, ready": {
			inputDatabase: map[fmt.Stringer]cbg.CBORMarshaler{
				stringer("/apples"): newInt(54),
			},
			test: func(t *testing.T, ss versioned.StateStore) {
				has, err := ss.Has(stringer("/apples"))
				require.True(t, has)
				require.NoError(t, err)
			},
		},
	}
	for testCase, data := range testCases {
		t.Run(testCase, func(t *testing.T) {
			ds := datastore.NewMapDatastore()
			ss := statestore.New(ds)
			if data.inputDatabase != nil {
				for key, value := range data.inputDatabase {
					err := ss.Begin(key, value)
					require.NoError(t, err)
				}
			}
			ms := migrationState{data.migrationErr}
			migratedSs := versioned.NewMigratedStateStore(ss, ms)
			data.test(t, migratedSs)
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

type stringer string

func (s stringer) String() string {
	return string(s)
}
