package brutus

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/palantir/stacktrace"
	"github.com/qri-io/jsonschema"
)

// JSONSchema creates a document validator from the given json schema - https://json-schema.org/
func JSONSchema(schemaContent []byte) (ValidatorHook, error) {
	schema := &jsonschema.Schema{}
	if err := json.Unmarshal(schemaContent, schema); err != nil {
		return nil, stacktrace.Propagate(err, "failed to decode json schema")
	}
	return func(ctx context.Context, _ *DB, d *DocChange) error {
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
