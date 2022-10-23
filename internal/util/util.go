package util

import (
	"encoding/json"
	"github.com/mitchellh/mapstructure"
)

func Decode(input any, output any) error {
	config := &mapstructure.DecoderConfig{
		Metadata:         nil,
		Result:           output,
		WeaklyTypedInput: true,
		TagName:          "json",
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
