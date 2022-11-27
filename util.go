package gokvkit

import (
	"encoding/json"
	"fmt"
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

func defaultAs(function Function, field string) string {
	if function != "" {
		return fmt.Sprintf("%s_%s", function, field)
	}
	return field
}
