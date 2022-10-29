package javascript

import (
	"bufio"
	"context"
	"github.com/autom8ter/wolverine"
	"github.com/autom8ter/wolverine/middleware"
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
func (f JSFunction) AggregateWare() middleware.AggregateWare {
	return func(aggregateFunc middleware.AggregateFunc) middleware.AggregateFunc {
		return func(ctx context.Context, collection *wolverine.Collection, query wolverine.AggregateQuery) (wolverine.Page, error) {
			input := map[string]any{
				"query":      mustMap(query),
				"collection": collection.Collection(),
			}
			metaCtx, _ := wolverine.GetContext(ctx)
			input["context"] = metaCtx.Map()
			val, err := f(input)
			if err != nil {
				return wolverine.Page{}, stacktrace.Propagate(err, "")
			}
			if val != nil {
				if err := wolverine.Decode(val, &query); err != nil {
					return wolverine.Page{}, stacktrace.Propagate(err, "")
				}
			}
			return aggregateFunc(ctx, collection, query)
		}
	}
}

// QueryWare converts the javascript function to a query middleware
// input: query(map), collection(string), context(map)
// sideEffects: the query is merged with the return value(map) from the script
func (f JSFunction) QueryWare() middleware.QueryWare {
	return func(queryFunc middleware.QueryFunc) middleware.QueryFunc {
		return func(ctx context.Context, collection *wolverine.Collection, query wolverine.Query) (wolverine.Page, error) {
			input := map[string]any{
				"query":      mustMap(query),
				"collection": collection.Collection(),
			}
			metaCtx, _ := wolverine.GetContext(ctx)
			input["context"] = metaCtx.Map()
			val, err := f(input)
			if err != nil {
				return wolverine.Page{}, stacktrace.Propagate(err, "")
			}
			if val != nil {
				if err := wolverine.Decode(val, &query); err != nil {
					return wolverine.Page{}, stacktrace.Propagate(err, "")
				}
			}
			return queryFunc(ctx, collection, query)
		}
	}
}

// PersistWare converts the javascript function to a persist middleware
// input: stateChange(map), collection(string), context(map)
// sideEffects: the stateChange is merged with the return value(map) from the script
func (f JSFunction) PersistWare() middleware.PersistWare {
	return func(persist middleware.PersistFunc) middleware.PersistFunc {
		return func(ctx context.Context, collection *wolverine.Collection, change wolverine.StateChange) error {
			input := map[string]any{
				"change":     mustMap(change),
				"collection": collection.Collection(),
			}
			metaCtx, _ := wolverine.GetContext(ctx)
			input["context"] = metaCtx.Map()
			val, err := f(input)
			if err != nil {
				return stacktrace.Propagate(err, "")
			}
			if m, ok := val.(map[string]any); ok {
				return stacktrace.Propagate(wolverine.Decode(&m, &change), "")
			}
			return persist(ctx, collection, change)
		}
	}
}

// ChangeStreamWare converts the javascript function to a change stream middleware
// input: stateChange(map), collection(string), context(map)
// sideEffects: the state change is merged with the return value(map) from the script
func (f JSFunction) ChangeStreamWare() middleware.ChangeStreamWare {
	return func(changeStream middleware.ChangeStreamFunc) middleware.ChangeStreamFunc {
		return func(ctx context.Context, collection *wolverine.Collection, fn wolverine.ChangeStreamHandler) error {
			return changeStream(ctx, collection, func(ctx context.Context, change wolverine.StateChange) error {
				input := map[string]any{
					"change":     mustMap(change),
					"collection": collection.Collection(),
				}
				metaCtx, _ := wolverine.GetContext(ctx)
				input["context"] = metaCtx.Map()
				val, err := f(input)
				if err != nil {
					return stacktrace.Propagate(err, "")
				}
				if m, ok := val.(map[string]any); ok {
					return stacktrace.Propagate(wolverine.Decode(&m, &change), "")
				}
				return nil
			})
		}
	}
}

func (f JSFunction) evalDocument(ctx context.Context, collection *wolverine.Collection, doc *wolverine.Document) (*wolverine.Document, error) {
	input := map[string]any{
		"id":         collection.GetPrimaryKey(doc),
		"document":   doc.Value(),
		"collection": collection.Collection(),
	}

	metaCtx, _ := wolverine.GetContext(ctx)
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

func toMap(o any) (map[string]any, error) {
	data := map[string]any{}
	if err := wolverine.Decode(&data, &o); err != nil {
		return nil, err
	}
	return data, nil
}

func mustMap(o any) map[string]any {
	data := map[string]any{}
	if err := wolverine.Decode(&data, &o); err != nil {
		panic(err)
	}
	return data
}
