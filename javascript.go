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

// JavascriptGlobal is a global variable injected into a javascript function (triggers/authorizers/etc)
type JavascriptGlobal string

const (
	// JavascriptGlobalDB is the global variable for the database instance - it is injected into all javascript functions
	// All methods, fields are available within the script
	JavascriptGlobalDB JavascriptGlobal = "db"
	// JavascriptGlobalCtx is the context of the request when the function is called - it is injected into all javascript functions
	// All methods, fields are available within the script
	JavascriptGlobalCtx JavascriptGlobal = "ctx"
	// JavascriptGlobalMeta is the context metadta of the request when the function is called - it is injected into all javascript functions
	// All methods, fields are available within the script
	JavascriptGlobalMeta JavascriptGlobal = "meta"
	// JavascriptGlobalTx is the transaction instance - it is injected into all javascript functions except ChangeStream Authorizers
	// All methods, fields are available within the script
	JavascriptGlobalTx JavascriptGlobal = "tx"
	// JavascriptGlobalSchema is the collection schema instance - it is injected into all javascript functions
	// All methods, fields are available within the script
	JavascriptGlobalSchema JavascriptGlobal = "schema"
	// JavascriptGlobalQuery is the query instance - it is injected into only Query-based based javascript functions (including foreach)
	// All methods, fields are available within the script
	JavascriptGlobalQuery JavascriptGlobal = "query"
	// JavascriptGlobalFilter is an array of where clauses - it is injected into ChangeStream based javascript functions
	// All methods, fields are available within the script
	JavascriptGlobalFilter JavascriptGlobal = "filter"
	// JavascriptGlobalDoc is a myjson document - it is injected into document based javascript functions
	// All methods, fields are available within the script
	JavascriptGlobalDoc JavascriptGlobal = "doc"
)

func getJavascriptVM(ctx context.Context, db Database, overrides map[string]any) (*goja.Runtime, error) {
	vm := goja.New()
	vm.SetFieldNameMapper(goja.TagFieldNameMapper("json", false))
	if err := vm.Set(string(JavascriptGlobalDB), db); err != nil {
		return nil, err
	}
	if err := vm.Set(string(JavascriptGlobalCtx), ctx); err != nil {
		return nil, err
	}
	if err := vm.Set(string(JavascriptGlobalMeta), ExtractMetadata(ctx)); err != nil {
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
		if v == nil {
			return false
		}
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
	"isZero":     funk.IsZero,
	"sum":        funk.Sum,
	"getOr":      funk.GetOrElse,
	"len":        func(v any) int { return len(cast.ToSlice(v)) },
	"toSlice":    cast.ToSlice,
	"toMap":      cast.ToStringMap,
	"toStr":      cast.ToString,
	"toInt":      cast.ToInt,
	"toFloat":    cast.ToFloat64,
	"toBool":     cast.ToBool,
	"toTime":     cast.ToTime,
	"toDuration": cast.ToDuration,
	"asDoc": func(v any) *Document {
		d, _ := NewDocumentFrom(v)
		return d
	},
	"indexOf": funk.IndexOf,
}
