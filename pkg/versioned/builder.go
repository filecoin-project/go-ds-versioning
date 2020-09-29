package versioned

import (
	"go.uber.org/multierr"

	versioning "github.com/filecoin-project/go-ds-versioning/pkg"
	"github.com/filecoin-project/go-ds-versioning/pkg/builder"
)

// Builder is just a versioned version of the migration builder
type Builder interface {
	Reversible(down versioning.MigrationFunc) Builder
	FilterKeys([]string) Builder
	Only([]string) Builder
	OldVersion(versioning.VersionKey) Builder
	Build() (versioning.VersionedMigration, error)
}

type versionedBuilder struct {
	base       builder.Builder
	newVersion versioning.VersionKey
	oldVersion versioning.VersionKey
}

// NewVersionedBuilder returns a new versioned builder for the given migration function
func NewVersionedBuilder(up versioning.MigrationFunc, newVersion versioning.VersionKey) Builder {
	return versionedBuilder{builder.NewMigrationBuilder(up), newVersion, ""}
}

func (vb versionedBuilder) Reversible(down versioning.MigrationFunc) Builder {
	return versionedBuilder{vb.base.Reversible(down), vb.newVersion, vb.oldVersion}
}

func (vb versionedBuilder) FilterKeys(keys []string) Builder {
	return versionedBuilder{vb.base.FilterKeys(keys), vb.newVersion, vb.oldVersion}
}

func (vb versionedBuilder) Only(keys []string) Builder {
	return versionedBuilder{vb.base.Only(keys), vb.newVersion, vb.oldVersion}
}

func (vb versionedBuilder) OldVersion(oldVersion versioning.VersionKey) Builder {
	return versionedBuilder{vb.base, vb.newVersion, oldVersion}
}

func (vb versionedBuilder) Build() (versioning.VersionedMigration, error) {
	baseMigration, err := vb.base.Build()
	if err != nil {
		return nil, err
	}
	return NewVersionedMigration(baseMigration, vb.oldVersion, vb.newVersion), nil
}

// BuilderList is a list of versioned builders that can be built into a single
// command to a VersionedMigrationList
type BuilderList []Builder

// Build creates a VersionedMigrationList from a list of VersionedBuilders in a single step
func (vbl BuilderList) Build() (versioning.VersionedMigrationList, error) {
	var migrations versioning.VersionedMigrationList
	var err error
	for _, builder := range vbl {
		migration, buildErr := builder.Build()
		if buildErr != nil {
			err = multierr.Append(err, buildErr)
		} else {
			migrations = append(migrations, migration)
		}
	}
	return migrations, err
}
