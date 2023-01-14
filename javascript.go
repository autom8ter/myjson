package myjson

import (
	"context"
	"reflect"
	"strings"
	"time"

	"github.com/dop251/goja"
	"github.com/google/uuid"
	"github.com/segmentio/ksuid"
	"github.com/spf13/cast"
	"github.com/thoas/go-funk"
)

func getJavascriptVM(ctx context.Context, db Database, overrides map[string]any) (*goja.Runtime, error) {
	vm := goja.New()
	vm.SetFieldNameMapper(goja.TagFieldNameMapper("json", true))
	if err := vm.Set("db", db); err != nil {
		return nil, err
	}
	if err := vm.Set("ctx", ctx); err != nil {
		return nil, err
	}
	if err := vm.Set("metadata", ExtractMetadata(ctx)); err != nil {
		return nil, err
	}
	for k, v := range builtins {
		if err := vm.Set(k, v); err != nil {
			return nil, err
		}
	}
	for k, v := range overrides {
		if err := vm.Set(k, v); err != nil {
			return nil, err
		}
	}
	return vm, nil
}

var builtins = map[string]any{
	"newDocumentFrom": NewDocumentFrom,
	"newDocument":     NewDocument,
	"ksuid": func() string {
		return ksuid.New().String()
	},
	"uuid": func() string {
		return uuid.New().String()
	},
	"now":    time.Now,
	"chunk":  funk.Chunk,
	"map":    funk.Map,
	"filter": funk.Filter,
	"find":   funk.Find,
	"contains": func(v any, e any) bool {
		switch v.(type) {
		case string:
			return strings.Contains(v.(string), cast.ToString(e))
		default:
			return funk.Contains(v, e)
		}
	},
	"equal":      funk.Equal,
	"deepEqual":  reflect.DeepEqual,
	"keys":       funk.Keys,
	"values":     funk.Values,
	"flatten":    funk.Flatten,
	"uniq":       funk.Uniq,
	"drop":       funk.Drop,
	"last":       funk.Last,
	"empty":      funk.IsEmpty,
	"notEmpty":   funk.NotEmpty,
	"difference": funk.Difference,
}
