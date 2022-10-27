package core

import (
	"bufio"
	"context"
	"github.com/autom8ter/wolverine/internal/util"
	"github.com/dop251/goja"
	"github.com/palantir/stacktrace"
	"strings"
)

// Javascript is a javascript function. Only a single function may be defined within the script.
type Javascript string

// NewJavascript creates a new javascript instance from the given script
func NewJavascript(script string) Javascript {
	return Javascript(script)
}

// Parse parses the sccript into a JSFunction
func (s Javascript) Parse() (JSFunction, error) {
	name := s.FunctionName()
	vm := goja.New()
	_, err := vm.RunString(string(s))
	if err != nil {
		return nil, stacktrace.Propagate(err, "")
	}
	var fn func(interface{}) (interface{}, error)
	if err := vm.ExportTo(vm.Get(name), &fn); err != nil {
		return nil, stacktrace.Propagate(err, "")
	}
	return fn, nil
}

// FunctionName returns the javascript function name
func (s Javascript) FunctionName() string {
	scanner := bufio.NewScanner(strings.NewReader(string(s)))
	scanner.Split(bufio.ScanWords)
	isNext := false
	for scanner.Scan() {
		word := scanner.Text()
		if word == "function" {
			isNext = true
			continue
		}
		if isNext {
			before, _, found := strings.Cut(word, "(")
			if found {
				return strings.TrimSpace(before)
			}
			isNext = false
		}
	}
	return ""
}

// JSFunction is a go representation of a javascript function
type JSFunction func(interface{}) (interface{}, error)

// AggregateWare converts the javascript function to an aggregate middleware
// input: query(map), collection(string), context(map)
// sideEffects: the aggregate query is merged with the return value from the script
func (f JSFunction) AggregateWare() AggregateWare {
	return func(aggregateFunc AggregateFunc) AggregateFunc {
		return func(ctx context.Context, collection *Collection, query AggregateQuery) (Page, error) {
			input := map[string]any{
				"query":      util.MustMap(query),
				"collection": collection.Collection(),
			}
			metaCtx, _ := GetContext(ctx)
			input["context"] = metaCtx.Map()
			val, err := f(input)
			if err != nil {
				return Page{}, stacktrace.Propagate(err, "")
			}
			if val != nil {
				if err := util.Decode(val, &query); err != nil {
					return Page{}, stacktrace.Propagate(err, "")
				}
			}
			return aggregateFunc(ctx, collection, query)
		}
	}
}

// QueryWare converts the javascript function to a query middleware
// input: query(map), collection(string), context(map)
// sideEffects: the query is merged with the return value(map) from the script
func (f JSFunction) QueryWare() QueryWare {
	return func(queryFunc QueryFunc) QueryFunc {
		return func(ctx context.Context, collection *Collection, query Query) (Page, error) {
			input := map[string]any{
				"query":      util.MustMap(query),
				"collection": collection.Collection(),
			}
			metaCtx, _ := GetContext(ctx)
			input["context"] = metaCtx.Map()
			val, err := f(input)
			if err != nil {
				return Page{}, stacktrace.Propagate(err, "")
			}
			if val != nil {
				if err := util.Decode(val, &query); err != nil {
					return Page{}, stacktrace.Propagate(err, "")
				}
			}
			return queryFunc(ctx, collection, query)
		}
	}
}

// SearchWare converts the javascript function to a search middleware
// input: query(map), collection(string), context(map)
// sideEffects: the search query is merged with the return value(map) from the script
func (f JSFunction) SearchWare() SearchWare {
	return func(searchWare SearchFunc) SearchFunc {
		return func(ctx context.Context, collection *Collection, query SearchQuery) (Page, error) {
			input := map[string]any{
				"query":      util.MustMap(query),
				"collection": collection.Collection(),
			}
			metaCtx, _ := GetContext(ctx)
			input["context"] = metaCtx.Map()
			val, err := f(input)
			if err != nil {
				return Page{}, stacktrace.Propagate(err, "")
			}
			if val != nil {
				if err := util.Decode(val, &query); err != nil {
					return Page{}, stacktrace.Propagate(err, "")
				}
			}
			return searchWare(ctx, collection, query)
		}
	}
}

// PersistWare converts the javascript function to a persist middleware
// input: stateChange(map), collection(string), context(map)
// sideEffects: the stateChange is merged with the return value(map) from the script
func (f JSFunction) PersistWare() PersistWare {
	return func(persist PersistFunc) PersistFunc {
		return func(ctx context.Context, collection *Collection, change StateChange) error {
			input := map[string]any{
				"change":     util.MustMap(change),
				"collection": collection.Collection(),
			}
			metaCtx, _ := GetContext(ctx)
			input["context"] = metaCtx.Map()
			val, err := f(input)
			if err != nil {
				return stacktrace.Propagate(err, "")
			}
			if m, ok := val.(map[string]any); ok {
				return stacktrace.Propagate(util.Decode(&m, &change), "")
			}
			return persist(ctx, collection, change)
		}
	}
}

// GetWare converts the javascript function to a get middleware
// input: id(string), collection(string), context(map)
// sideEffects: the return document is merged with the return value(map) from the script
func (f JSFunction) GetWare() GetWare {
	return func(get GetFunc) GetFunc {
		return func(ctx context.Context, collection *Collection, id string) (*Document, error) {
			doc, err := get(ctx, collection, id)
			if err != nil {
				return nil, stacktrace.Propagate(err, "")
			}
			return f.evalDocument(ctx, collection, doc)
		}
	}
}

// GetAllWare converts the javascript function to a get all middleware
// input: ids([]string), collection(string), context(map)
// sideEffects: the return documents are merged with the return value(map) from the script
func (f JSFunction) GetAllWare() GetAllWare {
	return func(get GetAllFunc) GetAllFunc {
		return func(ctx context.Context, collection *Collection, ids []string) ([]*Document, error) {
			docs, err := get(ctx, collection, ids)
			if err != nil {
				return nil, stacktrace.Propagate(err, "")
			}
			for _, doc := range docs {
				_, err := f.evalDocument(ctx, collection, doc)
				if err != nil {
					return nil, stacktrace.Propagate(err, "")
				}
			}
			return docs, nil
		}
	}
}

// ChangeStreamWare converts the javascript function to a change stream middleware
// input: stateChange(map), collection(string), context(map)
// sideEffects: the state change is merged with the return value(map) from the script
func (f JSFunction) ChangeStreamWare() ChangeStreamWare {
	return func(changeStream ChangeStreamFunc) ChangeStreamFunc {
		return func(ctx context.Context, collection *Collection, fn ChangeStreamHandler) error {
			return changeStream(ctx, collection, func(ctx context.Context, change StateChange) error {
				input := map[string]any{
					"change":     util.MustMap(change),
					"collection": collection.Collection(),
				}
				metaCtx, _ := GetContext(ctx)
				input["context"] = metaCtx.Map()
				val, err := f(input)
				if err != nil {
					return stacktrace.Propagate(err, "")
				}
				if m, ok := val.(map[string]any); ok {
					return stacktrace.Propagate(util.Decode(&m, &change), "")
				}
				return nil
			})
		}
	}
}

func (f JSFunction) evalDocument(ctx context.Context, collection *Collection, doc *Document) (*Document, error) {
	input := map[string]any{
		"id":         collection.GetPKey(doc),
		"document":   doc.Value(),
		"collection": collection.Collection(),
	}

	metaCtx, _ := GetContext(ctx)
	input["context"] = metaCtx.Map()
	val, err := f(input)
	if err != nil {
		return nil, stacktrace.Propagate(err, "")
	}
	if m, ok := val.(map[string]any); ok {
		if err := doc.SetAll(m); err != nil {
			return nil, stacktrace.Propagate(err, "")
		}
	}
	return doc, nil
}
