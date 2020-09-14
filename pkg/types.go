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
	Up(oldDs datastore.Batching, newDS datastore.Batching) error
}

// ReversableDatastoreMigration is
type ReversableDatastoreMigration interface {
	DatastoreMigration
	Down(newDs datastore.Batching, oldDS datastore.Batching) error
}

// MigrationBuilder is an interface for constructing migrations
type MigrationBuilder interface {
	Reversible(down MigrationFunc) MigrationBuilder
	FilterKeys([]string) MigrationBuilder
	Build() (DatastoreMigration, error)
}
