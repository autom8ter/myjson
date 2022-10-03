package wolverine

import (
	"context"
	"fmt"
	"sync"

	"github.com/autom8ter/machine/v4"
	"github.com/blevesearch/bleve"
	"github.com/dgraph-io/badger/v3"
	"github.com/palantir/stacktrace"
	"github.com/robfig/cron"
	"github.com/xeipuuv/gojsonschema"

	"github.com/autom8ter/wolverine/internal/prefix"
)

type db struct {
	Logger
	config      Config
	kv          *badger.DB
	fullText    map[string]bleve.Index
	mu          sync.RWMutex
	collections map[string]Collection
	cron        *cron.Cron
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
	collections := map[string]Collection{
		"system": {
			Name: "system",
		},
	}
	fulltext := map[string]bleve.Index{}
	for _, collection := range config.Collections {
		collections[collection.Name] = collection
		for _, i := range collection.Indexes {
			if i.FullText {
				indexMapping := bleve.NewIndexMapping()
				if len(i.Fields) > 0 && i.Fields[0] != "*" {
					//document := bleve.NewDocumentMapping()
					//for _, f := range i.Fields {
					//	disabled := bleve.NewDocumentDisabledMapping()
					//	document.AddSubDocumentMapping(f, disabled)
					//}
					//indexMapping.AddDocumentMapping("index", document)
				}

				if config.Path == "inmem" {
					i, err := bleve.NewMemOnly(indexMapping)
					if err != nil {
						return nil, err
					}
					fulltext[collection.Name] = i
				} else {
					path := fmt.Sprintf("%s/search/%s.bleve", config.Path, collection.Name)
					i, err := bleve.Open(path)
					if err == nil {
						fulltext[collection.Name] = i
					} else {
						i, err = bleve.New(path, indexMapping)
						if err != nil {
							return nil, err
						}
						fulltext[collection.Name] = i
					}
				}
			}
		}
		if collection.JSONSchema != "" {
			schema, err := gojsonschema.NewSchema(gojsonschema.NewStringLoader(collection.JSONSchema))
			if err != nil {
				return nil, stacktrace.Propagate(err, "")
			}
			collection.loadedSchema = schema
		}
	}
	d := &db{
		config:      *config,
		kv:          kv,
		fullText:    fulltext,
		mu:          sync.RWMutex{},
		collections: collections,
		cron:        cron.New(),
		machine:     machine.New(),
	}
	for _, c := range config.CronJobs {
		if err := d.cron.AddFunc(c.Schedule, func() {
			c.Function(ctx, d)
		}); err != nil {
			return nil, err
		}
	}
	d.Logger = config.Logger
	if d.Logger == nil {
		level := "info"
		if config.Debug {
			level = "debug"
		}
		lgger, err := NewLogger(level, map[string]any{})
		if err != nil {
			return nil, err
		}
		d.Logger = lgger
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

	d.cron.Start()
	return d, nil
}

func (i Index) prefix(collection string) *prefix.PrefixIndexRef {
	return prefix.NewPrefixedIndex(collection, i.Fields)
}

func chooseIndex(collection Collection, queryFields []string) *prefix.PrefixIndexRef {
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

func (d *db) getQueryPrefix(collection string, where []Where) []byte {
	var whereFields []string
	var whereValues = map[string]any{}
	for _, w := range where {
		if w.Op != "==" && w.Op != "eq" {
			continue
		}
		whereFields = append(whereFields, w.Field)
		whereValues[w.Field] = w.Value
	}
	return []byte(chooseIndex(d.collections[collection], whereFields).GetIndex(whereValues))
}
