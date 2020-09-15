package versioning

import "github.com/ipfs/go-datastore"

// MigrationFunc is a function to transform an single element of one type of data into
// a single element of another type of data. It has the following form:
// func<T extends cbg.CBORUnmarshaller, U extends cbg.CBORMarshaller>(old T) (new U, error)
type MigrationFunc interface{}

// DatastoreMigration can run a migration of a datastore that is a table
// of one kind of structured data and write it to a table that is another kind of
// structured data
type DatastoreMigration interface {
	Up(oldDs datastore.Batching, newDS datastore.Batching) ([]datastore.Key, error)
}

// ReversableDatastoreMigration is
type ReversableDatastoreMigration interface {
	DatastoreMigration
	Down(newDs datastore.Batching, oldDS datastore.Batching) ([]datastore.Key, error)
}

// MigrationBuilder is an interface for constructing migrations
type MigrationBuilder interface {
	Reversible(down MigrationFunc) MigrationBuilder
	FilterKeys([]string) MigrationBuilder
	Build() (DatastoreMigration, error)
}

// VersionKey is an identifier for a databased version
type VersionKey string

// VersionedMigration is a migration that migrates data in a single database
// between versions
type VersionedMigration interface {
	OldVersion() VersionKey
	NewVersion() VersionKey
	Up(ds datastore.Batching) ([]datastore.Key, error)
}

// ReversibleVersionedMigration is a migration that migrates data in a single database
// between versions, and can be reversed
type ReversibleVersionedMigration interface {
	VersionedMigration
	Down(ds datastore.Batching) ([]datastore.Key, error)
}

// VersionedMigrationList is a sortable list of versioned migrations
type VersionedMigrationList []VersionedMigration

// Len is the number of elements in the collection.
func (vml VersionedMigrationList) Len() int {
	return len(vml)
}

// Less reports whether the element with
// index i should sort before the element with index j.
func (vml VersionedMigrationList) Less(i int, j int) bool {
	return vml[i].NewVersion() < vml[j].NewVersion()
}

// Swap swaps the elements with indexes i and j.
func (vml VersionedMigrationList) Swap(i int, j int) {
	vml[i], vml[j] = vml[j], vml[i]
}
