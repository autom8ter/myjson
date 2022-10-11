package wolverine

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/autom8ter/machine/v4"
	"github.com/blevesearch/bleve"
	"github.com/dgraph-io/badger/v3"
	"github.com/palantir/stacktrace"
	"github.com/xeipuuv/gojsonschema"

	"github.com/autom8ter/wolverine/internal/prefix"
)

type db struct {
	config      Config
	kv          *badger.DB
	mu          sync.RWMutex
	collections sync.Map
	machine     machine.Machine
	fullText    bleve.IndexAlias
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
	if err := d.loadFullText(false); err != nil {
		return nil, err
	}
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

func (d *db) loadFullText(reload bool) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	indexMapping := bleve.NewIndexMapping()
	indexMapping.TypeField = "_collection"

	switch {
	case d.config.Path == "inmem":
		i, err := bleve.NewMemOnly(indexMapping)
		if err != nil {
			return stacktrace.Propagate(err, "")
		}
		d.fullText = bleve.NewIndexAlias(i)
	case reload:
		path := fmt.Sprintf("%s/search/index_%v.db", d.config.Path, time.Now().Unix())
		i, err := bleve.New(path, indexMapping)
		if err != nil {
			return stacktrace.Propagate(err, "")
		}
		d.fullText = bleve.NewIndexAlias(i)
	default:
		lastPath := d.getLastPath()
		i, err := bleve.Open(lastPath)
		if err == nil {
			d.fullText = bleve.NewIndexAlias(i)
		} else {
			i, err = bleve.New(lastPath, indexMapping)
			if err != nil {
				return stacktrace.Propagate(err, "")
			}
			d.fullText = bleve.NewIndexAlias(i)
		}
	}
	return nil
}

func (d *db) getLastPath() string {
	fileSystem := os.DirFS(fmt.Sprintf("%s/search", d.config.Path))
	var paths []string
	if err := fs.WalkDir(fileSystem, ".", func(path string, d fs.DirEntry, err error) error {
		if d.IsDir() {
			return nil
		}
		paths = append(paths, path)
		return nil
	}); err != nil {
		panic(err)
	}
	sort.Strings(paths)
	return paths[len(paths)-1]
}

func (d *db) loadCollections(ctx context.Context) error {
	collections, err := d.GetCollections(ctx)
	if err != nil {
		return stacktrace.Propagate(err, "")
	}
	sysCollection, err := LoadCollection(systemCollectionSchema)
	if err != nil {
		return stacktrace.Propagate(err, "")
	}
	collections = append(collections, sysCollection)
	for _, collection := range collections {
		if collection.Schema != "" {
			schema, err := gojsonschema.NewSchema(gojsonschema.NewStringLoader(collection.Schema))
			if err != nil {
				return stacktrace.Propagate(err, "")
			}
			collection.loadedSchema = schema
		}
		d.collections.Store(collection.Collection(), collection)
	}
	return nil
}

func chooseIndex(collection *Collection, queryFields []string) *prefix.PrefixIndexRef {
	//sort.Strings(queryFields)
	var targetIndex = Index{
		Fields: []string{"_id"},
	}
	for _, index := range collection.Indexes() {
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
	return targetIndex.prefix(collection.Collection())
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

func (d *db) collectionNames() []string {
	var names []string
	collections := d.getInmemCollections()
	for _, c := range collections {
		names = append(names, c.Collection())
	}
	return names
}
