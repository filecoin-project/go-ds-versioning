package utils

import (
	versioning "github.com/filecoin-project/go-ds-versioning/pkg"
	"github.com/ipfs/go-datastore"
)

// KeysForVersion wraps every key in a list with a version prefix
func KeysForVersion(from versioning.VersionKey, keys []datastore.Key) []datastore.Key {
	versionKeys := make([]datastore.Key, 0, len(keys))
	for _, key := range keys {
		versionKeys = append(versionKeys, datastore.NewKey(string(from)).Child(key))
	}
	return versionKeys
}
