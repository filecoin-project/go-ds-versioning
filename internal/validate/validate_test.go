package validate_test

import (
	"errors"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	cbg "github.com/whyrusleeping/cbor-gen"

	"github.com/filecoin-project/go-ds-versioning/internal/validate"
	versioning "github.com/filecoin-project/go-ds-versioning/pkg"
)

func TestCheckMigration(t *testing.T) {
	testCases := map[string]struct {
		migrateFunc        versioning.MigrationFunc
		expectedErr        error
		expectedInputType  reflect.Type
		expectedOutputType reflect.Type
	}{
		"not given a function": {
			migrateFunc: 8,
			expectedErr: errors.New("migration must be a function"),
		},
		"given a function that takes the wrong number of arguments": {
			migrateFunc: func() {},
			expectedErr: errors.New("migration must take exactly one argument"),
		},
		"given a function that produces the wrong number of outputs": {
			migrateFunc: func(c *cbg.CborInt) *cbg.CborBool {
				var out cbg.CborBool
				if *c != 0 {
					out = true
				}
				return &out
			},
			expectedErr: errors.New("migration must produce exactly two return values"),
		},
		"given a function that takes an input that isn't a cbor unmarshaller": {
			migrateFunc: func(c *uint64) (*cbg.CborBool, error) {
				var out cbg.CborBool
				if *c != 0 {
					out = true
				}
				return &out, nil
			},
			expectedErr: errors.New("input must be an unmarshallable CBOR struct"),
		},
		"given a function that produces an output that isn't a cbor marshaller": {
			migrateFunc: func(c *cbg.CborInt) (*bool, error) {
				var out cbg.CborBool
				if *c != 0 {
					out = true
				}
				return (*bool)(&out), nil
			},
			expectedErr: errors.New("first output must be an marshallable CBOR struct"),
		},
		"given a function that produces a second output that isn't an error": {
			migrateFunc: func(c *cbg.CborInt) (*cbg.CborBool, int) {
				var out cbg.CborBool
				if *c != 0 {
					out = true
				}
				return &out, 0
			},
			expectedErr: errors.New("second output must be an error interface"),
		},
		"given a function that matches the required format": {
			migrateFunc: func(c *cbg.CborInt) (*cbg.CborBool, error) {
				var out cbg.CborBool
				if *c != 0 {
					out = true
				}
				return &out, nil
			},
			expectedErr:        nil,
			expectedInputType:  reflect.TypeOf(new(cbg.CborInt)),
			expectedOutputType: reflect.TypeOf(new(cbg.CborBool)),
		},
	}
	for testCase, data := range testCases {
		t.Run(testCase, func(t *testing.T) {
			inputType, outputType, err := validate.CheckMigration(data.migrateFunc)
			if data.expectedErr == nil {
				require.NoError(t, err)
				require.Equal(t, data.expectedInputType, inputType)
				require.Equal(t, data.expectedOutputType, outputType)
			} else {
				require.EqualError(t, err, data.expectedErr.Error())
			}
		})
	}
}
