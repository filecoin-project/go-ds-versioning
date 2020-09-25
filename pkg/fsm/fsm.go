// Package fsm provides an abstraction on top of the fsm's defined by go-statemachine
// that allows you to make a group of finite state machines that tracks
// their own version and know how to migrate themselves to the target version
package fsm

import (
	"context"

	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"
	cbg "github.com/whyrusleeping/cbor-gen"

	"github.com/filecoin-project/go-statemachine/fsm"

	"github.com/filecoin-project/go-ds-versioning/internal/migrate"
	"github.com/filecoin-project/go-ds-versioning/internal/runner"
	"github.com/filecoin-project/go-ds-versioning/internal/utils"
	versioning "github.com/filecoin-project/go-ds-versioning/pkg"
)

type migratedFsm struct {
	fsm fsm.Group
	ms  versioning.MigrationState
}

// NewVersionedFSM sets takes a datastore, fsm parameters, migrations list, and target version, and returns
// an fsm whose functions will fail till it's migrated to the target version and a function to run migrations
func NewVersionedFSM(ds datastore.Batching, parameters fsm.Parameters, migrations versioning.VersionedMigrationList, target versioning.VersionKey) (fsm.Group, func(context.Context) error, error) {
	r := runner.NewRunner(ds, migrations, target, migrate.To)
	fsm, err := fsm.New(namespace.Wrap(ds, datastore.NewKey(string(target))), parameters)
	if err != nil {
		return nil, nil, err
	}
	return NewMigratedFSM(fsm, r), r.Migrate, nil
}

// NewMigratedFSM returns an fsm whose functions will fail until the migration state says its ready
func NewMigratedFSM(fsm fsm.Group, ms versioning.MigrationState) fsm.Group {
	return &migratedFsm{fsm, ms}
}

// Begin initiates tracking with a specific value for a given identifier
func (fsm *migratedFsm) Begin(id interface{}, userState interface{}) error {
	if err := fsm.ms.ReadyError(); err != nil {
		return err
	}
	return fsm.fsm.Begin(id, userState)
}

// Send sends the given event name and parameters to the state specified by id
// it will error if there are underlying state store errors or if the parameters
// do not match what is expected for the event name
func (fsm *migratedFsm) Send(id interface{}, name fsm.EventName, args ...interface{}) error {
	if err := fsm.ms.ReadyError(); err != nil {
		return err
	}
	return fsm.fsm.Send(id, name, args...)
}

// SendSync will block until the given event is actually processed, and
// will return an error if the transition was not possible given the current
// state
func (fsm *migratedFsm) SendSync(ctx context.Context, id interface{}, name fsm.EventName, args ...interface{}) error {
	if err := fsm.ms.ReadyError(); err != nil {
		return err
	}
	return fsm.fsm.SendSync(ctx, id, name, args...)
}

// Get gets state for a single state machine
func (fsm *migratedFsm) Get(id interface{}) fsm.StoredState {
	if err := fsm.ms.ReadyError(); err != nil {
		return &utils.NotReadyStoredState{Err: err}
	}
	return fsm.fsm.Get(id)
}

// GetSync will make sure all events present at the time of the call are processed before
// returning a value, which is read into out
func (fsm *migratedFsm) GetSync(ctx context.Context, id interface{}, value cbg.CBORUnmarshaler) error {
	if err := fsm.ms.ReadyError(); err != nil {
		return err
	}
	return fsm.fsm.GetSync(ctx, id, value)
}

// Has indicates whether there is data for the given state machine
func (fsm *migratedFsm) Has(id interface{}) (bool, error) {
	if err := fsm.ms.ReadyError(); err != nil {
		return false, err
	}
	return fsm.fsm.Has(id)
}

// List outputs states of all state machines in this group
// out: *[]StateT
func (fsm *migratedFsm) List(out interface{}) error {
	if err := fsm.ms.ReadyError(); err != nil {
		return err
	}
	return fsm.fsm.List(out)
}

// IsTerminated returns true if a StateType is in a FinalityState
func (fsm *migratedFsm) IsTerminated(out fsm.StateType) bool {
	// no database access here, so it's reasonable to not break if not ready
	return fsm.fsm.IsTerminated(out)
}

// Stop stops all state machines in this group
func (fsm *migratedFsm) Stop(ctx context.Context) error {
	if err := fsm.ms.ReadyError(); err != nil {
		return err
	}
	return fsm.fsm.Stop(ctx)
}
