package wolverine

import (
	"context"
	"fmt"
	"github.com/autom8ter/wolverine/schema"
	"io/fs"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/autom8ter/machine/v4"
	"github.com/blevesearch/bleve"
	"github.com/dgraph-io/badger/v3"
	"github.com/palantir/stacktrace"
)

type db struct {
	config      Config
	kv          *badger.DB
	mu          sync.RWMutex
	collections sync.Map
	machine     machine.Machine
	fullText    sync.Map
	aggIndexes  sync.Map
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
		return nil, stacktrace.Propagate(err, "")
	}

	d := &db{
		config:      *config,
		kv:          kv,
		mu:          sync.RWMutex{},
		collections: sync.Map{},
		machine:     machine.New(),
		aggIndexes:  sync.Map{},
	}
	if err := d.loadCollections(ctx); err != nil {
		return nil, stacktrace.Propagate(err, "")
	}
	for _, c := range d.getInmemCollections() {
		if err := d.loadFullText(c, false); err != nil {
			return nil, stacktrace.Propagate(err, "")
		}
	}

	if config.ReIndex {
		if err := d.ReIndex(ctx); err != nil {
			return nil, stacktrace.Propagate(err, "failed to reindex")
		}
	}
	if config.Migrate {
		if err := d.Migrate(ctx, config.Migrations); err != nil {
			return nil, stacktrace.Propagate(err, "migration failure")
		}
	}
	return d, nil
}

func (d *db) getFullText(collection string) bleve.Index {
	results, ok := d.fullText.Load(collection)
	if !ok {
		return nil
	}
	indexes, ok := results.([]bleve.Index)
	if !ok {
		return nil
	}
	if len(indexes) == 0 {
		return nil
	}
	return indexes[len(indexes)-1]
}

func (d *db) setFullText(collection string, index bleve.Index) {
	results, ok := d.fullText.Load(collection)
	if !ok {
		d.fullText.Store(collection, []bleve.Index{index})
		return
	}
	indexes, ok := results.([]bleve.Index)
	if !ok {
		d.fullText.Store(collection, []bleve.Index{index})
		return
	}
	indexes = append(indexes, index)
	d.fullText.Store(collection, indexes)
}

func (d *db) loadFullText(collection *schema.Collection, reindex bool) error {
	indexMapping := bleve.NewIndexMapping()
	indexMapping.TypeField = "_collection"
	newPath := fmt.Sprintf("%s/search/%s/index_%v.db", d.config.Path, collection.Collection(), time.Now().Unix())
	switch {
	case d.config.Path == "inmem" && !reindex:
		i, err := bleve.NewMemOnly(indexMapping)
		if err != nil {
			return stacktrace.Propagate(err, "failed to create %s search index", collection.Collection())
		}
		d.setFullText(collection.Collection(), i)
	case d.config.Path == "inmem" && reindex:
		i, err := bleve.NewMemOnly(indexMapping)
		if err != nil {
			return stacktrace.Propagate(err, "failed to create %s search index", collection.Collection())
		}
		d.setFullText(collection.Collection(), i)
	case reindex && d.config.Path != "inmem":
		i, err := bleve.New(newPath, indexMapping)
		if err != nil {
			return stacktrace.Propagate(err, "failed to create %s search index at path: %s", collection.Collection(), newPath)
		}
		d.setFullText(collection.Collection(), i)
	default:
		lastPath := d.getLastPath(collection)
		i, err := bleve.Open(lastPath)
		if err == nil {
			d.setFullText(collection.Collection(), i)
		} else {
			i, err = bleve.New(newPath, indexMapping)
			if err != nil {
				return stacktrace.Propagate(err, "failed to create %s search index at path: %s", collection.Collection(), newPath)
			}
			d.setFullText(collection.Collection(), i)
		}
	}
	return nil
}

func (d *db) getLastPath(collection *schema.Collection) string {
	fileSystem := os.DirFS(fmt.Sprintf("%s/search/%s", d.config.Path, collection.Collection()))
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
	if len(paths) == 0 {
		return ""
	}
	sort.Strings(paths)
	return paths[len(paths)-1]
}

func (d *db) loadCollections(ctx context.Context) error {
	collections, err := d.GetCollections(ctx)
	if err != nil {
		return stacktrace.Propagate(err, "")
	}
	sysCollection, err := schema.LoadCollection(systemCollectionSchema)
	if err != nil {
		return stacktrace.Propagate(err, "")
	}
	collections = append(collections, sysCollection)
	for _, collection := range collections {
		d.collections.Store(collection.Collection(), collection)
	}
	return nil
}

func (d *db) getInmemCollection(collection string) (*schema.Collection, bool) {
	c, ok := d.collections.Load(collection)
	if !ok {
		return nil, ok
	}
	return c.(*schema.Collection), ok
}

func (d *db) getInmemCollections() []*schema.Collection {
	var c []*schema.Collection
	d.collections.Range(func(key, value any) bool {
		c = append(c, value.(*schema.Collection))
		return true
	})
	return c
}

func (d *db) collectionNames() []string {
	var names []string
	collections := d.getInmemCollections()
	for _, c := range collections {
		names = append(names, c.Collection())
	}
	return names
}
