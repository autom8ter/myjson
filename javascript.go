package myjson

import (
	"context"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/dop251/goja"
	"github.com/google/uuid"
	"github.com/huandu/xstrings"
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
	for k, v := range JavascriptBuiltIns {
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

var JavascriptBuiltIns = map[string]any{
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
		switch v := v.(type) {
		case string:
			return strings.Contains(v, cast.ToString(e))
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
	"set":        funk.Set,
	"getOr":      funk.GetOrElse,
	"prune":      funk.Prune,
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
	"indexOf":    funk.IndexOf,
	"join":       strings.Join,
	"split":      strings.Split,
	"replace":    strings.ReplaceAll,
	"lower":      strings.ToLower,
	"upper":      strings.ToUpper,
	"trim":       strings.TrimSpace,
	"trimLeft":   strings.TrimLeft,
	"trimRight":  strings.TrimRight,
	"trimPrefix": strings.TrimPrefix,
	"trimSuffix": strings.TrimSuffix,
	"startsWith": strings.HasPrefix,
	"endsWith":   strings.HasSuffix,
	"camelCase":  xstrings.ToCamelCase,
	"snakeCase":  xstrings.ToSnakeCase,
	"kebabCase":  xstrings.ToKebabCase,
	"quote":      strconv.Quote,
	"unquote":    strconv.Unquote,
	"parseTime":  time.Parse,
	"since":      time.Since,
	"until":      time.Until,
	"after":      time.After,
	"unixMicro":  time.UnixMicro,
	"unixMilli":  time.UnixMilli,
	"unix":       time.Unix,
	"date":       time.Date,
}
