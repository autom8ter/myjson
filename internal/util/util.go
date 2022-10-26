package util

import (
	"encoding/json"
	"github.com/mitchellh/mapstructure"
)

func MustMap(o any) map[string]any {
	data := map[string]any{}
	if err := Decode(&data, &o); err != nil {
		panic(err)
	}
	return data
}

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

func JSONString(input any) string {
	bits, _ := json.Marshal(input)
	return string(bits)
}
