package util

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/mitchellh/mapstructure"
	"github.com/palantir/stacktrace"
	"github.com/spf13/cast"
	"gopkg.in/yaml.v2"
)

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
	default:
		bits, _ := json.Marshal(value)
		if len(bits) == 0 {
			bits = []byte(cast.ToString(value))
		}
		return bits
	}
}

func IsNil[T any](obj *T) bool {
	return obj == nil
}

func ToPtr[T any](obj T) *T {
	return &obj
}

func convertMap(m map[interface{}]interface{}) map[string]interface{} {
	res := map[string]interface{}{}
	for k, v := range m {
		switch v2 := v.(type) {
		case map[interface{}]interface{}:
			res[fmt.Sprint(k)] = convertMap(v2)
		default:
			res[fmt.Sprint(k)] = v
		}
	}
	return res
}

func YAMLToJSON(yamlContent []byte) ([]byte, error) {
	var body map[interface{}]interface{}
	if err := yaml.Unmarshal(yamlContent, &body); err != nil {
		return nil, stacktrace.Propagate(err, "failed to convert yaml to json")
	}
	jsonContent, err := json.Marshal(convertMap(body))
	if err != nil {
		return nil, stacktrace.Propagate(err, "")
	}
	return jsonContent, nil
}

func JSONToYAML(jsonContent []byte) ([]byte, error) {
	var body map[string]interface{}
	if err := json.Unmarshal(jsonContent, &body); err != nil {
		return nil, stacktrace.Propagate(err, "failed to convert json to yaml")
	}
	yamlContent, err := yaml.Marshal(body)
	if err != nil {
		return nil, stacktrace.Propagate(err, "")
	}
	return yamlContent, nil
}
