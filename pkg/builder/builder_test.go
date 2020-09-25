package builder_test

import (
	"bytes"
	"context"
	"errors"
	"testing"

	cborutil "github.com/filecoin-project/go-cbor-util"
	versioning "github.com/filecoin-project/go-ds-versioning/pkg"
	"github.com/filecoin-project/go-ds-versioning/pkg/builder"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	"github.com/stretchr/testify/require"
	cbg "github.com/whyrusleeping/cbor-gen"
)

func TestExecuteMigration(t *testing.T) {
	ctx := context.Background()
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

	testCases := map[string]struct {
		inputDatabase          map[string]*cbg.CborInt
		expectedOutputDatabase map[string]*cbg.CborInt
		upFunc                 versioning.MigrationFunc
		configure              func(builder.Builder) builder.Builder
		expectedErr            error
	}{
		"it works": {
			inputDatabase: map[string]*cbg.CborInt{
				"/apples":  &appleCount,
				"/oranges": &orangeCount,
			},
			expectedOutputDatabase: map[string]*cbg.CborInt{
				"/apples":  &changedAppleCount,
				"/oranges": &changedOrangeCount,
			},
			upFunc: migrateFunc,
		},
		"when reversible": {
			inputDatabase: map[string]*cbg.CborInt{
				"/apples":  &appleCount,
				"/oranges": &orangeCount,
			},
			expectedOutputDatabase: map[string]*cbg.CborInt{
				"/apples":  &changedAppleCount,
				"/oranges": &changedOrangeCount,
			},
			upFunc: migrateFunc,
			configure: func(builder builder.Builder) builder.Builder {
				return builder.Reversible(unmigrateFunc)
			},
		},
		"with key filtering": {
			inputDatabase: map[string]*cbg.CborInt{
				"/apples":  &appleCount,
				"/oranges": &orangeCount,
			},
			expectedOutputDatabase: map[string]*cbg.CborInt{
				"/apples": &changedAppleCount,
			},
			upFunc: migrateFunc,
			configure: func(builder builder.Builder) builder.Builder {
				return builder.FilterKeys([]string{"/oranges"})
			},
		},
		"down migration doesn't map up ": {
			inputDatabase: map[string]*cbg.CborInt{
				"/apples":  &appleCount,
				"/oranges": &orangeCount,
			},
			upFunc:      migrateFunc,
			expectedErr: errors.New("reversible function does not have inverse types"),
			configure: func(builder builder.Builder) builder.Builder {
				return builder.Reversible(func(c *cbg.CborInt) (*cbg.CborBool, error) {
					var out cbg.CborBool
					if *c != 0 {
						out = true
					}
					return &out, nil
				})
			},
		},
	}
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
			builder := builder.NewMigrationBuilder(data.upFunc)
			if data.configure != nil {
				builder = data.configure(builder)
			}
			migration, err := builder.Build()
			if data.expectedErr == nil {
				require.NoError(t, err)

				_, err = migration.Up(ctx, ds1, ds2)
				require.NoError(t, err)

				outputDatabase := make(map[string]*cbg.CborInt)
				res, err := ds2.Query(query.Query{})
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

				reversible, ok := migration.(versioning.ReversableDatastoreMigration)
				if ok {
					ds3 := datastore.NewMapDatastore()
					_, err = reversible.Down(ctx, ds2, ds3)
					require.NoError(t, err)

					reversedDatabase := make(map[string]*cbg.CborInt)
					res, err := ds3.Query(query.Query{})
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

				}
			} else {
				require.EqualError(t, err, data.expectedErr.Error())
			}
		})
	}

}
