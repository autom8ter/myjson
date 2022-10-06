package wolverine

import (
	"context"
	"fmt"
	"sync"

	"github.com/autom8ter/machine/v4"
	"github.com/blevesearch/bleve"
	"github.com/dgraph-io/badger/v3"
	"github.com/xeipuuv/gojsonschema"

	"github.com/autom8ter/wolverine/internal/prefix"
)

type db struct {
	config      Config
	kv          *badger.DB
	mu          sync.RWMutex
	collections sync.Map
	machine     machine.Machine
}

func New(ctx context.Context, cfg Config) (DB, error) {
	config := &cfg
	opts := badger.DefaultOptions(config.Path)
	if config.Path == "inmem" {
		opts.InMemory = true
		opts.Dir = ""
		opts.ValueDir = ""
	}
	opts = opts.WithLoggingLevel(badger.ERROR)
	kv, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	d := &db{
		config:      *config,
		kv:          kv,
		mu:          sync.RWMutex{},
		collections: sync.Map{},
		machine:     machine.New(),
	}
	d.collections.Store("system", &Collection{
		Name: "system",
	})
	if err := d.loadCollections(ctx); err != nil {
		return nil, err
	}
	if config.ReIndex {
		if err := d.ReIndex(ctx); err != nil {
			return nil, fmt.Errorf("failed to reindex: %s", err)
		}
	}
	if config.Migrate {
		if err := d.Migrate(ctx, config.Migrations); err != nil {
			return nil, err
		}
	}
	return d, nil
}

func (d *db) loadCollections(ctx context.Context) error {
	collections, err := d.GetCollections(ctx)
	if err != nil {
		return d.wrapErr(err, "")
	}
	for _, collection := range collections {
		for _, i := range collection.Indexes {
			if collection.fullText == nil && i.FullText {
				indexMapping := bleve.NewIndexMapping()
				if len(i.Fields) > 0 && i.Fields[0] != "*" {
					//document := bleve.NewDocumentMapping()
					//for _, f := range i.Fields {
					//	disabled := bleve.NewDocumentDisabledMapping()
					//	document.AddSubDocumentMapping(f, disabled)
					//}
					//indexMapping.AddDocumentMapping("index", document)
				}
				if d.config.Path == "inmem" {
					i, err := bleve.NewMemOnly(indexMapping)
					if err != nil {
						return d.wrapErr(err, "")
					}
					collection.fullText = i
				} else {
					path := fmt.Sprintf("%s/search/%s.bleve", d.config.Path, collection.Name)
					i, err := bleve.Open(path)
					if err == nil {
						collection.fullText = i
					} else {
						i, err = bleve.New(path, indexMapping)
						if err != nil {
							return d.wrapErr(err, "")
						}
						collection.fullText = i
					}
				}
			}
		}
		if collection.JSONSchema != "" {
			schema, err := gojsonschema.NewSchema(gojsonschema.NewStringLoader(collection.JSONSchema))
			if err != nil {
				return d.wrapErr(err, "")
			}
			collection.loadedSchema = schema
		}
		d.collections.Store(collection.Name, collection)
	}
	return nil
}

func chooseIndex(collection *Collection, queryFields []string) *prefix.PrefixIndexRef {
	//sort.Strings(queryFields)
	var targetIndex = Index{
		Fields:   []string{"_id"},
		FullText: false,
	}
	for _, index := range collection.Indexes {
		if len(index.Fields) != len(queryFields) {
			continue
		}
		match := true
		for i, f := range queryFields {
			if index.Fields[i] != f {
				match = false
			}
		}
		if match {
			targetIndex = index
		}
	}
	return targetIndex.prefix(collection.Name)
}

func (d *db) getInmemCollection(collection string) (*Collection, bool) {
	c, ok := d.collections.Load(collection)
	if !ok {
		return nil, ok
	}
	return c.(*Collection), ok
}

func (d *db) getInmemCollections() []*Collection {
	var c []*Collection
	d.collections.Range(func(key, value any) bool {
		c = append(c, value.(*Collection))
		return true
	})
	return c
}

func (d *db) getQueryPrefix(collection string, where []Where) []byte {
	c, ok := d.getInmemCollection(collection)
	if !ok {
		return nil
	}
	var whereFields []string
	var whereValues = map[string]any{}
	for _, w := range where {
		if w.Op != "==" && w.Op != "eq" {
			continue
		}
		whereFields = append(whereFields, w.Field)
		whereValues[w.Field] = w.Value
	}
	return []byte(chooseIndex(c, whereFields).GetIndex(whereValues))
}
