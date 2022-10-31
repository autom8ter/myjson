package wolverine

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/palantir/stacktrace"
	"github.com/qri-io/jsonschema"
)

// JSONSchema is a json schema document validator. It will panic if the schemaContent is invalid.
func JSONSchema(schemaContent []byte) DocumentValidator {
	schema := &jsonschema.Schema{}
	if err := json.Unmarshal(schemaContent, schema); err != nil {
		panic(stacktrace.Propagate(err, "failed to decode json schema"))
	}
	return func(ctx context.Context, d *Document) error {
		kerrs, err := schema.ValidateBytes(ctx, d.Bytes())
		if err != nil {
			return err
		}
		if len(kerrs) > 0 {
			return fmt.Errorf(JSONString(&kerrs))
		}
		return nil
	}
}
