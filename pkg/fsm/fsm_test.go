package fsm_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	cbg "github.com/whyrusleeping/cbor-gen"

	"github.com/filecoin-project/go-statemachine/fsm"

	"github.com/filecoin-project/go-ds-versioning/internal/utils"
	versioning "github.com/filecoin-project/go-ds-versioning/pkg"
	versioned "github.com/filecoin-project/go-ds-versioning/pkg/fsm"
)

func TestFSM(t *testing.T) {
	testCases := map[string]struct {
		migrationErr error
		fsm          testFsm
		test         func(t *testing.T, fsm fsm.Group)
	}{
		"Begin, not ready": {
			migrationErr: versioning.ErrMigrationsNotRun,
			test: func(t *testing.T, fsm fsm.Group) {
				require.EqualError(t, fsm.Begin(struct{}{}, struct{}{}), versioning.ErrMigrationsNotRun.Error())
			},
		},
		"Begin, ready": {
			fsm: testFsm{
				BeginErr: errors.New("something went wrong"),
			},
			test: func(t *testing.T, fsm fsm.Group) {
				require.EqualError(t, fsm.Begin(struct{}{}, struct{}{}), "something went wrong")
			},
		},
		"Send, not ready": {
			migrationErr: versioning.ErrMigrationsNotRun,
			test: func(t *testing.T, fsm fsm.Group) {
				require.EqualError(t, fsm.Send(struct{}{}, struct{}{}), versioning.ErrMigrationsNotRun.Error())
			},
		},
		"Send, ready": {
			fsm: testFsm{
				SendErr: errors.New("something went wrong"),
			},
			test: func(t *testing.T, fsm fsm.Group) {
				require.EqualError(t, fsm.Send(struct{}{}, struct{}{}), "something went wrong")
			},
		},
		"SendSync, not ready": {
			migrationErr: versioning.ErrMigrationsNotRun,
			test: func(t *testing.T, fsm fsm.Group) {
				require.EqualError(t, fsm.SendSync(context.Background(), struct{}{}, struct{}{}), versioning.ErrMigrationsNotRun.Error())
			},
		},
		"SendSync, ready": {
			fsm: testFsm{
				SendSyncErr: errors.New("something went wrong"),
			},
			test: func(t *testing.T, fsm fsm.Group) {
				require.EqualError(t, fsm.SendSync(context.Background(), struct{}{}, struct{}{}), "something went wrong")
			},
		},
		"Get, not ready": {
			migrationErr: errors.New("something went wrong with migrations"),
			test: func(t *testing.T, fsm fsm.Group) {
				storedState := fsm.Get(struct{}{})
				require.EqualError(t, storedState.End(), "something went wrong with migrations")
			},
		},
		"Get, ready": {
			fsm: testFsm{
				StoredStateErr: errors.New("something went wrong"),
			},
			test: func(t *testing.T, fsm fsm.Group) {
				storedState := fsm.Get(struct{}{})
				require.EqualError(t, storedState.End(), "something went wrong")
			},
		},
		"GetSync, not ready": {
			migrationErr: versioning.ErrMigrationsNotRun,
			test: func(t *testing.T, fsm fsm.Group) {
				require.EqualError(t, fsm.GetSync(context.Background(), struct{}{}, nil), versioning.ErrMigrationsNotRun.Error())
			},
		},
		"GetSync, ready": {
			fsm: testFsm{
				GetSyncErr: errors.New("something went wrong"),
			},
			test: func(t *testing.T, fsm fsm.Group) {
				require.EqualError(t, fsm.GetSync(context.Background(), struct{}{}, nil), "something went wrong")
			},
		},
		"Has, not ready": {
			migrationErr: versioning.ErrMigrationsNotRun,
			test: func(t *testing.T, fsm fsm.Group) {
				has, err := fsm.Has(struct{}{})
				require.False(t, has)
				require.EqualError(t, err, versioning.ErrMigrationsNotRun.Error())
			},
		},
		"Has, ready": {
			fsm: testFsm{
				HasResult: true,
			},
			test: func(t *testing.T, fsm fsm.Group) {
				has, err := fsm.Has(struct{}{})
				require.True(t, has)
				require.NoError(t, err)
			},
		},
		"List, not ready": {
			migrationErr: versioning.ErrMigrationsNotRun,
			test: func(t *testing.T, fsm fsm.Group) {
				require.EqualError(t, fsm.List([]struct{}{}), versioning.ErrMigrationsNotRun.Error())
			},
		},
		"List, ready": {
			fsm: testFsm{
				ListErr: errors.New("something went wrong"),
			},
			test: func(t *testing.T, fsm fsm.Group) {
				require.EqualError(t, fsm.List([]struct{}{}), "something went wrong")
			},
		},
		"IsTerminated, not ready": {
			migrationErr: versioning.ErrMigrationsNotRun,
			fsm: testFsm{
				IsTerminatedResult: true,
			},
			test: func(t *testing.T, fsm fsm.Group) {
				terminated := fsm.IsTerminated(struct{}{})
				require.True(t, terminated)
			},
		},
		"IsTerminated, ready": {
			fsm: testFsm{
				IsTerminatedResult: true,
			},
			test: func(t *testing.T, fsm fsm.Group) {
				terminated := fsm.IsTerminated(struct{}{})
				require.True(t, terminated)
			},
		},
		"Stop, not ready": {
			migrationErr: versioning.ErrMigrationsNotRun,
			test: func(t *testing.T, fsm fsm.Group) {
				require.EqualError(t, fsm.Stop(context.Background()), versioning.ErrMigrationsNotRun.Error())
			},
		},
		"Stop, ready": {
			fsm: testFsm{
				StopErr: errors.New("something went wrong"),
			},
			test: func(t *testing.T, fsm fsm.Group) {
				require.EqualError(t, fsm.Stop(context.Background()), "something went wrong")
			},
		},
	}
	for testCase, data := range testCases {
		t.Run(testCase, func(t *testing.T) {
			ms := migrationState{data.migrationErr}
			fsm := versioned.NewMigratedFSM(&data.fsm, ms)
			data.test(t, fsm)
		})
	}
}

type migrationState struct {
	err error
}

func (ms migrationState) ReadyError() error {
	return ms.err
}

type testFsm struct {
	BeginErr           error
	SendErr            error
	SendSyncErr        error
	StoredStateErr     error
	GetSyncErr         error
	ListErr            error
	HasResult          bool
	HasErr             error
	IsTerminatedResult bool
	StopErr            error
}

func (tfsm *testFsm) Begin(id interface{}, userState interface{}) error {
	return tfsm.BeginErr
}

// Send sends the given event name and parameters to the state specified by id
// it will error if there are underlying state store errors or if the parameters
// do not match what is expected for the event name
func (tfsm *testFsm) Send(id interface{}, name fsm.EventName, args ...interface{}) (err error) {
	return tfsm.SendErr
}

// SendSync will block until the given event is actually processed, and
// will return an error if the transition was not possible given the current
// state
func (tfsm *testFsm) SendSync(ctx context.Context, id interface{}, name fsm.EventName, args ...interface{}) (err error) {
	return tfsm.SendSyncErr
}

// Get gets state for a single state machine
func (tfsm *testFsm) Get(id interface{}) fsm.StoredState {
	return &utils.NotReadyStoredState{Err: tfsm.StoredStateErr}
}

// GetSync will make sure all events present at the time of the call are processed before
// returning a value, which is read into out
func (tfsm *testFsm) GetSync(ctx context.Context, id interface{}, value cbg.CBORUnmarshaler) error {
	return tfsm.GetSyncErr
}

// Has indicates whether there is data for the given state machine
func (tfsm *testFsm) Has(id interface{}) (bool, error) {
	return tfsm.HasResult, tfsm.HasErr
}

// List outputs states of all state machines in this group
// out: *[]StateT
func (tfsm *testFsm) List(out interface{}) error {
	return tfsm.ListErr
}

// IsTerminated returns true if a StateType is in a FinalityState
func (tfsm *testFsm) IsTerminated(out fsm.StateType) bool {
	return tfsm.IsTerminatedResult
}

// Stop stops all state machines in this group
func (tfsm *testFsm) Stop(ctx context.Context) error {
	return tfsm.StopErr
}
