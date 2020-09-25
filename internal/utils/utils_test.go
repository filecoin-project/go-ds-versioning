package utils_test

import (
	"testing"

	"github.com/ipfs/go-datastore"
	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/go-ds-versioning/internal/utils"
	versioning "github.com/filecoin-project/go-ds-versioning/pkg"
)

func TestKeysForVersion(t *testing.T) {
	version := versioning.VersionKey("123")
	testKeys := []datastore.Key{
		datastore.NewKey("/apples"), datastore.NewKey("/oranges"),
	}
	expectedOutput := []datastore.Key{
		datastore.NewKey("/123/apples"), datastore.NewKey("/123/oranges"),
	}
	require.Equal(t, expectedOutput, utils.KeysForVersion(version, testKeys))
}
