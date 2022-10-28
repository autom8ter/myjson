package javascript

import (
	"bufio"
	"context"
	"github.com/autom8ter/wolverine/core"
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
func (f JSFunction) AggregateWare() core.AggregateWare {
	return func(aggregateFunc core.AggregateFunc) core.AggregateFunc {
		return func(ctx context.Context, collection *core.Collection, query core.AggregateQuery) (core.Page, error) {
			input := map[string]any{
				"query":      util.MustMap(query),
				"collection": collection.Collection(),
			}
			metaCtx, _ := core.GetContext(ctx)
			input["context"] = metaCtx.Map()
			val, err := f(input)
			if err != nil {
				return core.Page{}, stacktrace.Propagate(err, "")
			}
			if val != nil {
				if err := util.Decode(val, &query); err != nil {
					return core.Page{}, stacktrace.Propagate(err, "")
				}
			}
			return aggregateFunc(ctx, collection, query)
		}
	}
}

// QueryWare converts the javascript function to a query middleware
// input: query(map), collection(string), context(map)
// sideEffects: the query is merged with the return value(map) from the script
func (f JSFunction) QueryWare() core.QueryWare {
	return func(queryFunc core.QueryFunc) core.QueryFunc {
		return func(ctx context.Context, collection *core.Collection, query core.Query) (core.Page, error) {
			input := map[string]any{
				"query":      util.MustMap(query),
				"collection": collection.Collection(),
			}
			metaCtx, _ := core.GetContext(ctx)
			input["context"] = metaCtx.Map()
			val, err := f(input)
			if err != nil {
				return core.Page{}, stacktrace.Propagate(err, "")
			}
			if val != nil {
				if err := util.Decode(val, &query); err != nil {
					return core.Page{}, stacktrace.Propagate(err, "")
				}
			}
			return queryFunc(ctx, collection, query)
		}
	}
}

// SearchWare converts the javascript function to a search middleware
// input: query(map), collection(string), context(map)
// sideEffects: the search query is merged with the return value(map) from the script
func (f JSFunction) SearchWare() core.SearchWare {
	return func(searchWare core.SearchFunc) core.SearchFunc {
		return func(ctx context.Context, collection *core.Collection, query core.SearchQuery) (core.Page, error) {
			input := map[string]any{
				"query":      util.MustMap(query),
				"collection": collection.Collection(),
			}
			metaCtx, _ := core.GetContext(ctx)
			input["context"] = metaCtx.Map()
			val, err := f(input)
			if err != nil {
				return core.Page{}, stacktrace.Propagate(err, "")
			}
			if val != nil {
				if err := util.Decode(val, &query); err != nil {
					return core.Page{}, stacktrace.Propagate(err, "")
				}
			}
			return searchWare(ctx, collection, query)
		}
	}
}

// PersistWare converts the javascript function to a persist middleware
// input: stateChange(map), collection(string), context(map)
// sideEffects: the stateChange is merged with the return value(map) from the script
func (f JSFunction) PersistWare() core.PersistWare {
	return func(persist core.PersistFunc) core.PersistFunc {
		return func(ctx context.Context, collection *core.Collection, change core.StateChange) error {
			input := map[string]any{
				"change":     util.MustMap(change),
				"collection": collection.Collection(),
			}
			metaCtx, _ := core.GetContext(ctx)
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
func (f JSFunction) GetWare() core.GetWare {
	return func(get core.GetFunc) core.GetFunc {
		return func(ctx context.Context, collection *core.Collection, id string) (*core.Document, error) {
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
func (f JSFunction) GetAllWare() core.GetAllWare {
	return func(get core.GetAllFunc) core.GetAllFunc {
		return func(ctx context.Context, collection *core.Collection, ids []string) (core.Documents, error) {
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
func (f JSFunction) ChangeStreamWare() core.ChangeStreamWare {
	return func(changeStream core.ChangeStreamFunc) core.ChangeStreamFunc {
		return func(ctx context.Context, collection *core.Collection, fn core.ChangeStreamHandler) error {
			return changeStream(ctx, collection, func(ctx context.Context, change core.StateChange) error {
				input := map[string]any{
					"change":     util.MustMap(change),
					"collection": collection.Collection(),
				}
				metaCtx, _ := core.GetContext(ctx)
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

func (f JSFunction) evalDocument(ctx context.Context, collection *core.Collection, doc *core.Document) (*core.Document, error) {
	input := map[string]any{
		"id":         collection.GetPrimaryKey(doc),
		"document":   doc.Value(),
		"collection": collection.Collection(),
	}

	metaCtx, _ := core.GetContext(ctx)
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
