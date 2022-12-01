package gokvkit

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/palantir/stacktrace"
	"github.com/qri-io/jsonschema"
	"github.com/tidwall/gjson"
)

// JSONSchema creates a document validator hook from the given json schema - https://json-schema.org/
func JSONSchema(schemaContent []byte) (ValidatorHook, error) {
	schema := &jsonschema.Schema{}
	if err := json.Unmarshal(schemaContent, schema); err != nil {
		return ValidatorHook{}, stacktrace.Propagate(err, "failed to decode json schema")
	}
	return ValidatorHook{
		Name: fmt.Sprintf("%s.jsonschema", gjson.Get(string(schemaContent), "title").String()),
		Func: func(ctx context.Context, _ *DB, command *Command) error {
			switch command.Action {
			case UpdateDocument, CreateDocument, SetDocument:
				if command.Change != nil {
					kerrs, err := schema.ValidateBytes(ctx, command.Change.Bytes())
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
