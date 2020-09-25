package runner_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ipfs/go-datastore"
	"github.com/stretchr/testify/assert"
	"go.uber.org/atomic"

	"github.com/filecoin-project/go-ds-versioning/internal/runner"
	versioning "github.com/filecoin-project/go-ds-versioning/pkg"
)

func TestMigrate(t *testing.T) {

	ctx := context.Background()
	testCases := map[string]struct {
		instancesToExecute  int
		param               func() interface{}
		inspectRunnerBefore func(*testing.T, *runner.Runner)
		getRunMigrations    func(interface{}) runner.RunMigrationsFunc
		inspectRunnerAfter  func(*testing.T, *runner.Runner)
		inspectParamAfter   func(*testing.T, interface{})
		expectedErr         error
	}{
		"it will only run migrations once": {
			instancesToExecute: 10,
			param: func() interface{} {
				return atomic.NewUint64(0)
			},
			getRunMigrations: func(param interface{}) runner.RunMigrationsFunc {
				count := param.(*atomic.Uint64)
				return func(context.Context, datastore.Batching, versioning.VersionedMigrationList, versioning.VersionKey) (versioning.VersionKey, error) {
					count.Inc()
					return versioning.VersionKey(""), nil
				}
			},
			inspectParamAfter: func(t *testing.T, param interface{}) {
				count := param.(*atomic.Uint64)
				assert.Equal(t, uint64(1), count.Load())
			},
		},
		"ready error changes once migrations run": {
			instancesToExecute: 10,
			inspectRunnerBefore: func(t *testing.T, r *runner.Runner) {
				assert.EqualError(t, r.ReadyError(), versioning.ErrMigrationsNotRun.Error())
			},
			getRunMigrations: func(param interface{}) runner.RunMigrationsFunc {
				return func(context.Context, datastore.Batching, versioning.VersionedMigrationList, versioning.VersionKey) (versioning.VersionKey, error) {
					return versioning.VersionKey(""), nil
				}
			},
			inspectRunnerAfter: func(t *testing.T, r *runner.Runner) {
				assert.NoError(t, r.ReadyError())
			},
		},
		"error in migrations": {
			instancesToExecute: 10,
			inspectRunnerBefore: func(t *testing.T, r *runner.Runner) {
				assert.EqualError(t, r.ReadyError(), versioning.ErrMigrationsNotRun.Error())
			},
			getRunMigrations: func(param interface{}) runner.RunMigrationsFunc {
				return func(context.Context, datastore.Batching, versioning.VersionedMigrationList, versioning.VersionKey) (versioning.VersionKey, error) {
					return versioning.VersionKey(""), errors.New("something went wrong")
				}
			},
			inspectRunnerAfter: func(t *testing.T, r *runner.Runner) {
				assert.EqualError(t, r.ReadyError(), "Error migrating database: something went wrong")
			},
			expectedErr: errors.New("something went wrong"),
		},
		"migrations get stuck": {
			instancesToExecute: 10,
			inspectRunnerBefore: func(t *testing.T, r *runner.Runner) {
				assert.EqualError(t, r.ReadyError(), versioning.ErrMigrationsNotRun.Error())
			},
			param: func() interface{} {
				return make(chan struct{})
			},
			getRunMigrations: func(param interface{}) runner.RunMigrationsFunc {
				blocker := param.(chan struct{})
				return func(context.Context, datastore.Batching, versioning.VersionedMigrationList, versioning.VersionKey) (versioning.VersionKey, error) {
					<-blocker
					return versioning.VersionKey(""), nil
				}
			},
			inspectParamAfter: func(t *testing.T, param interface{}) {
				blocker := param.(chan struct{})
				close(blocker)
			},
			inspectRunnerAfter: func(t *testing.T, r *runner.Runner) {
				assert.EqualError(t, r.ReadyError(), versioning.ErrMigrationsNotRun.Error())
			},
			expectedErr: versioning.ErrContextCancelled,
		},
	}
	for testCase, data := range testCases {
		t.Run(testCase, func(t *testing.T) {
			ds := datastore.NewMapDatastore()
			key := versioning.VersionKey("3")
			var migrations versioning.VersionedMigrationList
			ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
			defer cancel()
			var param interface{}
			if data.param != nil {
				param = data.param()
			}
			runMigrations := data.getRunMigrations(param)
			runner := runner.NewRunner(ds, migrations, key, runMigrations)
			var wg sync.WaitGroup
			for i := 0; i < data.instancesToExecute; i++ {
				wg.Add(1)
				go func() {
					if data.inspectRunnerBefore != nil {
						data.inspectRunnerBefore(t, runner)
					}
					wg.Done()
				}()
			}
			wg.Wait()
			for i := 0; i < data.instancesToExecute; i++ {
				wg.Add(1)
				go func() {
					err := runner.Migrate(ctx)
					if data.inspectRunnerAfter != nil {
						data.inspectRunnerAfter(t, runner)
					}

					if data.expectedErr != nil {
						assert.EqualError(t, err, data.expectedErr.Error())
					} else {
						assert.NoError(t, err)
					}
					wg.Done()
				}()
			}
			wg.Wait()
			if data.inspectParamAfter != nil {
				data.inspectParamAfter(t, param)
			}
		})
	}
}
