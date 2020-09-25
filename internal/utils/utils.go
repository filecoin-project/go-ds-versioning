package utils

import (
	versioning "github.com/filecoin-project/go-ds-versioning/pkg"
	"github.com/ipfs/go-datastore"
	cbg "github.com/whyrusleeping/cbor-gen"
)

// KeysForVersion wraps every key in a list with a version prefix
func KeysForVersion(from versioning.VersionKey, keys []datastore.Key) []datastore.Key {
	versionKeys := make([]datastore.Key, 0, len(keys))
	for _, key := range keys {
		versionKeys = append(versionKeys, datastore.NewKey(string(from)).Child(key))
	}
	return versionKeys
}

// NotReadyStoredState just returns errors that migrations that have not been run
type NotReadyStoredState struct {
	Err error
}

// End returns a migration error
func (nrss *NotReadyStoredState) End() error {
	return nrss.Err
}

// Get returns a migration error
func (nrss *NotReadyStoredState) Get(out cbg.CBORUnmarshaler) error {
	return nrss.Err
}

// Mutate returns a migration error
func (nrss *NotReadyStoredState) Mutate(mutator interface{}) error {
	return nrss.Err
}
