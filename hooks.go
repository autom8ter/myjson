package gokvkit

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/palantir/stacktrace"
	"github.com/qri-io/jsonschema"
	"github.com/tidwall/gjson"
)

// ValidatorHook is a hook function used to validate all new and updated documents being persisted to a collection
type ValidatorHook struct {
	Name string
	Func func(ctx context.Context, db *DB, change *DocChange) error
}

// Valid returns nil if the hook is valid
func (v ValidatorHook) Valid() error {
	if v.Name == "" {
		return stacktrace.NewError("empty hook name")
	}
	if v.Func == nil {
		return stacktrace.NewError("empty hook function")
	}
	return nil
}

// SideEffectHook is a hook function triggered whenever a document changes
type SideEffectHook struct {
	Name string
	Func func(ctx context.Context, db *DB, change *DocChange) (*DocChange, error)
}

// Valid returns nil if the hook is valid
func (v SideEffectHook) Valid() error {
	if v.Name == "" {
		return stacktrace.NewError("empty hook name")
	}
	if v.Func == nil {
		return stacktrace.NewError("empty hook function")
	}
	return nil
}

// WhereHook is a hook function triggered before queries/scans are executed. They may be used for a varietey of purposes (ex: query authorization hooks)
type WhereHook struct {
	Name string
	Func func(ctx context.Context, db *DB, where []Where) ([]Where, error)
}

// Valid returns nil if the hook is valid
func (v WhereHook) Valid() error {
	if v.Name == "" {
		return stacktrace.NewError("empty hook name")
	}
	if v.Func == nil {
		return stacktrace.NewError("empty hook function")
	}
	return nil
}

// ReadHook is a hook function triggered on each passing result of a read-based request
type ReadHook struct {
	Name string
	Func func(ctx context.Context, db *DB, document *Document) (*Document, error)
}

// Valid returns nil if the hook is valid
func (v ReadHook) Valid() error {
	if v.Name == "" {
		return stacktrace.NewError("empty hook name")
	}
	if v.Func == nil {
		return stacktrace.NewError("empty hook function")
	}
	return nil
}

// JSONSchema creates a document validator hook from the given json schema - https://json-schema.org/
func JSONSchema(schemaContent []byte) (ValidatorHook, error) {
	schema := &jsonschema.Schema{}
	if err := json.Unmarshal(schemaContent, schema); err != nil {
		return ValidatorHook{}, stacktrace.Propagate(err, "failed to decode json schema")
	}
	return ValidatorHook{
		Name: fmt.Sprintf("%s.jsonschema", gjson.Get(string(schemaContent), "title").String()),
		Func: func(ctx context.Context, _ *DB, d *DocChange) error {
			switch d.Action {
			case Update, Create, Set:
				if d.After != nil {
					kerrs, err := schema.ValidateBytes(ctx, d.After.Bytes())
					if err != nil {
						return err
					}
					if len(kerrs) > 0 {
						return fmt.Errorf(JSONString(&kerrs))
					}
				}
			}
			return nil
		},
	}, nil
}

// MustJSONSchema creates a document validator from the given json schema - https://json-schema.org/
// It will panic if the schemaContent is invalid.
func MustJSONSchema(schemaContent []byte) ValidatorHook {
	s, err := JSONSchema(schemaContent)
	if err != nil {
		panic(err)
	}
	return s
}
