package myjson

import (
	"context"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
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
	// newDocumentFrom is a helper function to create a new document from a map
	"newDocumentFrom": NewDocumentFrom,
	// newDocument is a helper function to create an empty document
	"newDocument": NewDocument,
	// ksuid is a helper function to create a new ksuid id (time sortable id)
	"ksuid": func() string {
		return ksuid.New().String()
	},
	// uuid is a helper function to create a new uuid
	"uuid": func() string {
		return uuid.New().String()
	},
	// now is a helper function to get the current time object
	"now": time.Now,
	// chunk is a helper function to chunk an array of values
	"chunk": funk.Chunk,
	// map is a helper function to map an array of values
	"map": funk.Map,
	// filter is a helper function to filter an array of values
	"filter": funk.Filter,
	// find is a helper function to find an element in an array of values
	"find": funk.Find,
	// contains is a helper function to check if an array of values contains a value or if a string contains a substring
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
	// equals is a helper function to check if two objects are equal
	"equal": funk.Equal,
	// equals is a helper function to check if two objects are equal
	"deepEqual": reflect.DeepEqual,
	// keys is a helper function to get the keys of a object
	"keys": funk.Keys,
	// values is a helper function to get the values of an object
	"values": funk.Values,
	// flatten is a helper function to flatten an array of values
	"flatten": funk.Flatten,
	// uniq is a helper function to get the unique values of an array
	"uniq": funk.Uniq,
	// drop is a helper function to drop the first n elements of an array
	"drop": funk.Drop,
	// last is a helper function to get the last element of an array
	"last": funk.Last,
	// empty is a helper function to check if a value is empty
	"empty": funk.IsEmpty,
	// notEmpty is a helper function to check if a value is not empty
	"notEmpty": funk.NotEmpty,
	// difference is a helper function to get the difference between two arrays
	"difference": funk.Difference,
	// isZero is a helper function to check if a value is zero
	"isZero": funk.IsZero,
	// sum is a helper function to sum an array of values
	"sum": funk.Sum,
	// set is a helper function to set a value in an object
	"set": funk.Set,
	// getOr is a helper function to get a value from an object or return a default value
	"getOr": funk.GetOrElse,
	// prune is a helper function to prune an object of empty values
	"prune": funk.Prune,
	// len is a helper function to get the length of an array
	"len": func(v any) int { return len(cast.ToSlice(v)) },
	// toSlice is a helper function to cast a value to a slice
	"toSlice": cast.ToSlice,
	// toMap is a helper function to cast a value to a map
	"toMap": cast.ToStringMap,
	// toStr is a helper function to cast a value to a string
	"toStr": cast.ToString,
	// toInt is a helper function to cast a value to an int
	"toInt": cast.ToInt,
	// toFloat is a helper function to cast a value to a float
	"toFloat": cast.ToFloat64,
	// toBool is a helper function to cast a value to a bool
	"toBool": cast.ToBool,
	// toTime is a helper function to cast a value to a date
	"toTime": cast.ToTime,
	// toDuration is a helper function to cast a value to a duration
	"toDuration": cast.ToDuration,
	// asDoc is a helper function to cast a value to a document
	"asDoc": func(v any) *Document {
		d, _ := NewDocumentFrom(v)
		return d
	},
	// indexOf is a helper function to get the index of a value in an array
	"indexOf": funk.IndexOf,
	// join is a helper function to join an array of values
	"join": strings.Join,
	// split is a helper function to split a string
	"split": strings.Split,
	// replace is a helper function to replace a string
	"replace": strings.ReplaceAll,
	// lower is a helper function to lower a string
	"lower": strings.ToLower,
	// upper is a helper function to upper a string
	"upper": strings.ToUpper,
	// trim is a helper function to trim a string
	"trim": strings.TrimSpace,
	// trimLeft is a helper function to trim a string
	"trimLeft": strings.TrimLeft,
	// trimRight is a helper function to trim a string
	"trimRight": strings.TrimRight,
	// trimPrefix is a helper function to trim a string
	"trimPrefix": strings.TrimPrefix,
	// trimSuffix is a helper function to trim a string
	"trimSuffix": strings.TrimSuffix,
	// startsWith is a helper function to check if a string starts with a substring
	"startsWith": strings.HasPrefix,
	// endsWith is a helper function to check if a string ends with a substring
	"endsWith": strings.HasSuffix,
	// camelCase is a helper function to convert a string to camelCase
	"camelCase": xstrings.ToCamelCase,
	// snakeCase is a helper function to convert a string to snake_case
	"snakeCase": xstrings.ToSnakeCase,
	// kebabCase is a helper function to convert a string to kebab-case
	"kebabCase": xstrings.ToKebabCase,
	// quote is a helper function to quote a string
	"quote": strconv.Quote,
	// unquote is a helper function to unquote a string
	"unquote": strconv.Unquote,
	// parseTime is a helper function to parse a time
	"parseTime": time.Parse,
	// since is a helper function to get the duration since a time
	"since": time.Since,
	// until is a helper function to get the duration until a time
	"until": time.Until,
	// after is a helper function to get the duration after a time
	"after": time.After,
	// unixMicro is a helper function to get the time from a unix micro timestamp
	"unixMicro": time.UnixMicro,
	// unixMilli is a helper function to get the time from a unix milli timestamp
	"unixMilli": time.UnixMilli,
	// unix is a helper function to get the time from a unix timestamp
	"unix": time.Unix,
	// date is a helper function to get the date from a timestamp
	"date": time.Date,
	// sha1 is a helper function to get the sha1 hash of a string
	"sha1": func(v any) string {
		h := sha1.New()
		h.Write([]byte(cast.ToString(v)))
		return hex.EncodeToString(h.Sum(nil))
	},
	// sha256 is a helper function to get the sha256 hash of a string
	"sha256": func(v any) string {
		h := sha256.New()
		h.Write([]byte(cast.ToString(v)))
		return hex.EncodeToString(h.Sum(nil))
	},
	// sha512 is a helper function to get the sha512 hash of a string
	"sha512": func(v any) string {
		h := sha512.New()
		h.Write([]byte(cast.ToString(v)))
		return hex.EncodeToString(h.Sum(nil))
	},
	// md5 is a helper function to get the md5 hash of a string
	"md5": func(v any) string {
		h := md5.New()
		h.Write([]byte(cast.ToString(v)))
		return hex.EncodeToString(h.Sum(nil))
	},
	// base64Encode is a helper function to encode a string to base64
	"base64Encode": func(v any) string {
		return base64.StdEncoding.EncodeToString([]byte(cast.ToString(v)))
	},
	// base64Decode is a helper function to decode a string from base64
	"base64Decode": func(v any) (string, error) {
		b, err := base64.StdEncoding.DecodeString(cast.ToString(v))
		return string(b), err
	},
	// jsonEncode is a helper function to encode a value to json
	"jsonEncode": func(v any) (string, error) {
		b, err := json.Marshal(v)
		return string(b), err
	},
	// jsonDecode is a helper function to decode a value from json
	"jsonDecode": func(v string) (any, error) {
		var out any
		err := json.Unmarshal([]byte(v), &out)
		return out, err
	},
	// fetch is a helper function to fetch a url
	"fetch": func(request map[string]any) (map[string]any, error) {
		method := cast.ToString(request["method"])
		if method == "" {
			return nil, fmt.Errorf("missing 'method'")
		}
		url := cast.ToString(request["url"])
		if url == "" {
			return nil, fmt.Errorf("missing 'url'")
		}
		req, err := http.NewRequest(method, url, nil)
		if err != nil {
			return nil, err
		}
		if headers, ok := request["headers"]; ok {
			for k, v := range cast.ToStringMap(headers) {
				req.Header.Set(k, cast.ToString(v))
			}
		}
		if queryParams, ok := request["query"]; ok {
			q := req.URL.Query()
			for k, v := range cast.ToStringMap(queryParams) {
				q.Set(k, cast.ToString(v))
			}
			req.URL.RawQuery = q.Encode()
		}
		if body, ok := request["body"]; ok {
			req.Body = ioutil.NopCloser(strings.NewReader(cast.ToString(body)))
		}

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		b, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		return map[string]any{
			"status":  resp.StatusCode,
			"headers": resp.Header,
			"body":    string(b),
		}, nil
	},
}
