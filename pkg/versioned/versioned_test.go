package versioned_test

import (
	"bytes"
	"testing"

	cborutil "github.com/filecoin-project/go-cbor-util"
	"github.com/filecoin-project/go-ds-versioning/internal/utils"
	versioning "github.com/filecoin-project/go-ds-versioning/pkg"
	"github.com/filecoin-project/go-ds-versioning/pkg/builder"
	"github.com/filecoin-project/go-ds-versioning/pkg/versioned"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	"github.com/stretchr/testify/require"
	cbg "github.com/whyrusleeping/cbor-gen"
)

func TestExecuteMigration(t *testing.T) {
	var appleCount = cbg.CborInt(30)
	var orangeCount = cbg.CborInt(0)
	var changedAppleCount = cbg.CborInt(37)
	var changedOrangeCount = cbg.CborInt(7)

	migrateFunc := func(c *cbg.CborInt) (*cbg.CborInt, error) {
		newCount := *c + 7
		return &newCount, nil
	}

	unmigrateFunc := func(c *cbg.CborInt) (*cbg.CborInt, error) {
		oldCount := *c - 7
		return &oldCount, nil
	}

	migration, err := builder.NewMigrationBuilder(migrateFunc).Reversible(unmigrateFunc).Build()
	require.NoError(t, err)

	testCases := map[string]struct {
		inputDatabase          map[string]*cbg.CborInt
		expectedOutputDatabase map[string]*cbg.CborInt
		versionedMigration     versioning.ReversibleVersionedMigration
	}{
		"regular": {
			inputDatabase: map[string]*cbg.CborInt{
				"/1/apples":  &appleCount,
				"/1/oranges": &orangeCount,
			},
			expectedOutputDatabase: map[string]*cbg.CborInt{
				"/2/apples":  &changedAppleCount,
				"/2/oranges": &changedOrangeCount,
			},
			versionedMigration: versioned.NewVersionedMigration(migration, "1", "2").(versioning.ReversibleVersionedMigration),
		},
		"initial migration": {
			inputDatabase: map[string]*cbg.CborInt{
				"/apples":  &appleCount,
				"/oranges": &orangeCount,
			},
			expectedOutputDatabase: map[string]*cbg.CborInt{
				"/1/apples":  &changedAppleCount,
				"/1/oranges": &changedOrangeCount,
			},
			versionedMigration: versioned.NewInitialVersionedMigration(migration, "1").(versioning.ReversibleVersionedMigration),
		},
	}
	for testCase, data := range testCases {
		t.Run(testCase, func(t *testing.T) {
			ds1 := datastore.NewMapDatastore()
			if data.inputDatabase != nil {
				for key, value := range data.inputDatabase {
					buf := new(bytes.Buffer)
					err := value.MarshalCBOR(buf)
					require.NoError(t, err)
					err = ds1.Put(datastore.NewKey(key), buf.Bytes())
					require.NoError(t, err)
				}
			}

			keys, err := data.versionedMigration.Up(ds1)
			require.NoError(t, err)

			batch, err := ds1.Batch()
			require.NoError(t, err)
			keys = utils.KeysForVersion(data.versionedMigration.OldVersion(), keys)
			for _, key := range keys {
				require.NoError(t, batch.Delete(key))
			}
			require.NoError(t, batch.Commit())

			outputDatabase := make(map[string]*cbg.CborInt)
			res, err := ds1.Query(query.Query{})
			require.NoError(t, err)
			defer res.Close()
			for {
				res, ok := res.NextSync()
				if !ok {
					break
				}
				require.NoError(t, res.Error)
				var out cbg.CborInt
				err := cborutil.ReadCborRPC(bytes.NewReader(res.Value), &out)
				require.NoError(t, err)
				outputDatabase[res.Key] = &out
			}
			require.Equal(t, data.expectedOutputDatabase, outputDatabase)

			keys, err = data.versionedMigration.Down(ds1)
			require.NoError(t, err)

			batch, err = ds1.Batch()
			require.NoError(t, err)
			keys = utils.KeysForVersion(data.versionedMigration.NewVersion(), keys)
			for _, key := range keys {
				require.NoError(t, batch.Delete(key))
			}
			require.NoError(t, batch.Commit())

			reversedDatabase := make(map[string]*cbg.CborInt)
			res, err = ds1.Query(query.Query{})
			require.NoError(t, err)
			defer res.Close()
			for {
				res, ok := res.NextSync()
				if !ok {
					break
				}
				require.NoError(t, res.Error)
				var out cbg.CborInt
				err := cborutil.ReadCborRPC(bytes.NewReader(res.Value), &out)
				require.NoError(t, err)
				reversedDatabase[res.Key] = &out
			}
			require.Equal(t, data.inputDatabase, reversedDatabase)
		})
	}

}
