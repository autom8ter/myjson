package util

import (
	"encoding/binary"
	"encoding/json"
	"time"

	"github.com/autom8ter/myjson/errors"
	"github.com/ghodss/yaml"
	"github.com/go-playground/validator/v10"
	"github.com/mitchellh/mapstructure"
	"github.com/spf13/cast"
)

var validate = validator.New()

func ValidateStruct(val any) error {
	return errors.Wrap(validate.Struct(val), errors.Validation, "")
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

func EncodeIndexValue(value any) []byte {
	if value == nil {
		return []byte("")
	}
	switch value := value.(type) {
	case bool:
		return EncodeIndexValue(cast.ToString(value))
	case string:
		return []byte(value)
	case int, int64, int32, float64, float32, uint64, uint32, uint16:
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, cast.ToUint64(value))
		return buf
	case time.Time:
		return EncodeIndexValue(value.UnixNano())
	case time.Duration:
		return EncodeIndexValue(int(value))
	default:
		return EncodeIndexValue(JSONString(value))
	}
}

func YAMLToJSON(yamlContent []byte) ([]byte, error) {
	if isJSON(string(yamlContent)) {
		return yamlContent, nil
	}
	return yaml.YAMLToJSON(yamlContent)
}

func JSONToYAML(jsonContent []byte) ([]byte, error) {
	return yaml.JSONToYAML(jsonContent)
}

func isJSON(str string) bool {
	var js json.RawMessage
	return json.Unmarshal([]byte(str), &js) == nil
}

func RemoveElement[T any](index int, results []T) []T {
	return append(results[:index], results[index+1:]...)
}
