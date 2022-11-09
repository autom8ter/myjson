package brutus

import (
	"context"
	"encoding/json"
	"github.com/mitchellh/mapstructure"
)

func compareField(field string, i, j *Document) bool {
	iFieldVal := i.result.Get(field)
	jFieldVal := j.result.Get(field)
	switch i.result.Get(field).Value().(type) {
	case bool:
		return iFieldVal.Bool() && !jFieldVal.Bool()
	case float64:
		return iFieldVal.Float() > jFieldVal.Float()
	case string:
		return iFieldVal.String() > jFieldVal.String()
	default:
		return JSONString(iFieldVal.Value()) > JSONString(jFieldVal.Value())
	}
}

// MustDecode decodes the input into the output based on json tags - it panics on error
func MustDecode(input any, output any) {
	if err := Decode(input, output); err != nil {
		panic(err)
	}
}

// Decode decodes the input into the output based on json tags
func Decode(input any, output any) error {
	config := &mapstructure.DecoderConfig{
		WeaklyTypedInput:     true,
		Result:               output,
		TagName:              "json",
		IgnoreUntaggedFields: true,
	}

	decoder, err := mapstructure.NewDecoder(config)
	if err != nil {
		return err
	}

	return decoder.Decode(input)
}

// JSONString returns a json string of the input
func JSONString(input any) string {
	bits, _ := json.Marshal(input)
	return string(bits)
}

func isInternalCtx(ctx context.Context) bool {
	meta, _ := GetContext(ctx)
	isInternal, ok := meta.Get("_internal")
	if ok && isInternal == true {
		return true
	}
	return false
}
