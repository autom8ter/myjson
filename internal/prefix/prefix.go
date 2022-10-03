package prefix

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/nqd/flat"
	"github.com/spf13/cast"
)

type PrefixIndexRef struct {
	collection    string
	initialPrefix []string
	fields        []string
}

func NewPrefixedIndex(collection string, fields []string) *PrefixIndexRef {
	return &PrefixIndexRef{
		collection:    collection,
		initialPrefix: []string{"index", collection},
		fields:        fields,
	}
}

func PrimaryIndex(collection string) *PrefixIndexRef {
	return NewPrefixedIndex(collection, []string{"_id"})
}

func PrimaryKey(collection string, id string) string {
	return NewPrefixedIndex(collection, []string{"_id"}).GetIndex(map[string]any{
		"_id": id,
	})
}

func (d PrefixIndexRef) GetIndex(value any) string {
	fields := map[string]any{}
	switch value := value.(type) {
	case map[string]any:
		fields = value
	default:
		bits, _ := json.Marshal(value)
		json.Unmarshal(bits, &fields)
	}
	fields, _ = flat.Flatten(fields, nil)
	var path []string
	for _, p := range d.initialPrefix {
		path = append(path, p)
	}
	for _, k := range d.fields {
		if v, ok := fields[k]; ok {
			path = append(path, fmt.Sprintf("%s%s", toStringHash(k), toStringHash(v)))
		} else {
			path = append(path, fmt.Sprintf("%s%s", toStringHash(k), ""))
		}
	}
	return strings.Join(path, ".")
}

func toStringHash(value any) string {

	if value == nil {
		return ""
	}
	bits, _ := json.Marshal(value)
	if len(bits) == 0 {
		bits = []byte(cast.ToString(value))
	}
	return string(bits)
	// s := sha1.New()
	//s.Write(bits)
	//return base64.StdEncoding.EncodeToString(s.Sum(nil))
}

// Cache
func Cache(key string) []byte {
	return []byte(fmt.Sprintf("cache.%s", key))
}
