package versioned_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	cbg "github.com/whyrusleeping/cbor-gen"

	versioning "github.com/filecoin-project/go-ds-versioning/pkg"
	"github.com/filecoin-project/go-ds-versioning/pkg/builder"
	"github.com/filecoin-project/go-ds-versioning/pkg/versioned"
)

func TestVersionedBuilderTest(t *testing.T) {
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
		builder           versioned.Builder
		expectedErr       error
		expectedMigration versioning.VersionedMigration
	}{
		"normal case": {
			builder:           versioned.NewVersionedBuilder(migrateFunc, "2").Reversible(unmigrateFunc).OldVersion("1"),
			expectedErr:       nil,
			expectedMigration: versioned.NewVersionedMigration(migration, "1", "2"),
		},
		"no old version": {
			builder:           versioned.NewVersionedBuilder(migrateFunc, "2").Reversible(unmigrateFunc),
			expectedErr:       nil,
			expectedMigration: versioned.NewInitialVersionedMigration(migration, "2"),
		},
		"builder error": {
			builder:           versioned.NewVersionedBuilder(7, "2"),
			expectedErr:       errors.New("migration must be a function"),
			expectedMigration: nil,
		},
	}
	for testCase, data := range testCases {
		t.Run(testCase, func(t *testing.T) {
			migration, err := data.builder.Build()
			if data.expectedErr == nil {
				require.NoError(t, err)
				require.Equal(t, data.expectedMigration, migration)
			} else {
				require.EqualError(t, err, data.expectedErr.Error())
			}
		})
	}
}
