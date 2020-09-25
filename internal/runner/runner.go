package runner

import (
	"context"
	"fmt"
	"sync"

	versioning "github.com/filecoin-project/go-ds-versioning/pkg"
	"github.com/ipfs/go-datastore"
	"go.uber.org/atomic"
)

// RunMigrationsFunc is a function that runs migrations
type RunMigrationsFunc func(ctx context.Context, ds datastore.Batching, migrations versioning.VersionedMigrationList, target versioning.VersionKey) (versioning.VersionKey, error)

// Runner executes a migrations exactly once
// and can queried for status of that migration and any migration errors
type Runner struct {
	doMigration    sync.Once
	migrationsDone chan struct{}
	migrationError atomic.Error
	migrations     versioning.VersionedMigrationList
	target         versioning.VersionKey
	ready          atomic.Bool
	ds             datastore.Batching
	runMigrations  RunMigrationsFunc
}

// NewRunner returns a new runner instance for the given datastore, migrations, and target
func NewRunner(ds datastore.Batching, migrations versioning.VersionedMigrationList, target versioning.VersionKey, runMigrations RunMigrationsFunc) *Runner {
	return &Runner{
		ds:             ds,
		migrations:     migrations,
		target:         target,
		runMigrations:  runMigrations,
		migrationsDone: make(chan struct{}),
	}
}

// Migrate executes the migration, if it has not already been executed
func (m *Runner) Migrate(ctx context.Context) error {
	go func() {
		m.doMigration.Do(func() {
			_, err := m.runMigrations(ctx, m.ds, m.migrations, m.target)
			m.migrationError.Store(err)
			m.ready.Store(true)
			close(m.migrationsDone)
		})
	}()
	select {
	case <-m.migrationsDone:
	case <-ctx.Done():
		return versioning.ErrContextCancelled
	}
	return m.migrationError.Load()
}

// ReadyError returns the ready state of the migration -
// either nil for ready or err for not ready or a migration error
func (m *Runner) ReadyError() error {
	if err := m.migrationError.Load(); err != nil {
		return fmt.Errorf("Error migrating database: %w", err)
	}
	if m.ready.Load() {
		return nil
	}
	return versioning.ErrMigrationsNotRun
}
