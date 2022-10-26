package core

import (
	"bufio"
	"context"
	"github.com/autom8ter/wolverine/internal/util"
	"github.com/dop251/goja"
	"github.com/palantir/stacktrace"
	"strings"
)

type Javascript string

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

type JSFunction func(interface{}) (interface{}, error)

func (f JSFunction) AggregateWare() AggregateWare {
	return func(aggregateFunc AggregateFunc) AggregateFunc {
		return func(ctx context.Context, collection *Collection, query AggregateQuery) (Page, error) {
			input := map[string]any{
				"query":      util.MustMap(query),
				"collection": collection.Collection(),
			}
			metaCtx, ok := GetContext(ctx)
			if ok {
				input["context"] = metaCtx.Map()
			}
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

func (f JSFunction) QueryWare() QueryWare {
	return func(queryFunc QueryFunc) QueryFunc {
		return func(ctx context.Context, collection *Collection, query Query) (Page, error) {
			input := map[string]any{
				"query":      util.MustMap(query),
				"collection": collection.Collection(),
			}
			metaCtx, ok := GetContext(ctx)
			if ok {
				input["context"] = metaCtx.Map()
			}
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

func (f JSFunction) SearchWare() SearchWare {
	return func(searchWare SearchFunc) SearchFunc {
		return func(ctx context.Context, collection *Collection, query SearchQuery) (Page, error) {
			input := map[string]any{
				"query":      util.MustMap(query),
				"collection": collection.Collection(),
			}
			metaCtx, ok := GetContext(ctx)
			if ok {
				input["context"] = metaCtx.Map()
			}
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

func (f JSFunction) PersistWare() PersistWare {
	return func(persist PersistFunc) PersistFunc {
		return func(ctx context.Context, collection *Collection, change StateChange) error {
			input := map[string]any{
				"change":     util.MustMap(change),
				"collection": collection.Collection(),
			}
			metaCtx, ok := GetContext(ctx)
			if ok {
				input["context"] = metaCtx.Map()
			}
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

func (f JSFunction) ChangeStreamWare() ChangeStreamWare {
	return func(changeStream ChangeStreamFunc) ChangeStreamFunc {
		return func(ctx context.Context, collection *Collection, fn ChangeStreamHandler) error {

			return changeStream(ctx, collection, func(ctx context.Context, change StateChange) error {
				input := map[string]any{
					"change":     util.MustMap(change),
					"collection": collection.Collection(),
				}
				metaCtx, ok := GetContext(ctx)
				if ok {
					input["context"] = metaCtx.Map()
				}
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
		"id":         collection.GetDocumentID(doc),
		"document":   doc.Value(),
		"collection": collection.Collection(),
	}
	metaCtx, ok := GetContext(ctx)
	if ok {
		input["context"] = metaCtx.Map()
	}
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
