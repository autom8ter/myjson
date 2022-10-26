package wolverine

import (
	"context"
	_ "embed"
	"fmt"
	"github.com/autom8ter/machine/v4"
	"github.com/autom8ter/wolverine/core"
	"github.com/autom8ter/wolverine/errors"
	"github.com/autom8ter/wolverine/schema"
	"github.com/autom8ter/wolverine/store"
	"github.com/blevesearch/bleve"
	"github.com/dgraph-io/badger/v3"
	"github.com/palantir/stacktrace"
	"golang.org/x/sync/errgroup"
	"io"
	"os"
	"sync"
)

// Config configures a database instance
type Config struct {
	// StoragePath is the path to database storage.
	// Leave empty to operate the database in memory only.
	StoragePath string `json:"storagePath"`
	// Collections are the json document collections supported by the DB - At least one is required.
	Collections []*schema.Collection `json:"collections"`
	middlewares []core.Middleware
}

func (c Config) AddMiddleware(m core.Middleware) Config {
	middlewares := append(c.middlewares, m)
	return Config{
		StoragePath: c.StoragePath,
		Collections: c.Collections,
		middlewares: middlewares,
	}
}

// LoadConfig loads a config instance from the spefied storeage path and a directory containing the collection schemas
func LoadConfig(storeagePath string, schemaDir string) (Config, error) {
	collections, err := schema.LoadCollectionsFromDir(schemaDir)
	if err != nil {
		return Config{}, stacktrace.Propagate(err, "")
	}
	return Config{
		StoragePath: storeagePath,
		Collections: collections,
	}, nil
}

type DB struct {
	config       Config
	kv           *badger.DB
	mu           sync.RWMutex
	schema       *schema.Schema
	machine      machine.Machine
	collections  []*Collection
	errorHandler func(collection string, err error)
	closers      []func() error
}

func New(ctx context.Context, cfg Config) (*DB, error) {
	if len(cfg.Collections) == 0 {
		return nil, stacktrace.NewErrorWithCode(errors.ErrTODO, "zero collections configured")
	}
	config := &cfg
	opts := badger.DefaultOptions(config.StoragePath)
	if config.StoragePath == "" {
		opts.InMemory = true
		opts.Dir = ""
		opts.ValueDir = ""
	}
	opts = opts.WithLoggingLevel(badger.ERROR)
	kv, err := badger.Open(opts)
	if err != nil {
		return nil, stacktrace.Propagate(err, "")
	}

	d := &DB{
		config:  *config,
		kv:      kv,
		mu:      sync.RWMutex{},
		schema:  schema.NewSchema(nil),
		machine: machine.New(),
	}
	if d.errorHandler == nil {
		d.errorHandler = func(collection string, err error) {
			fmt.Printf("[%s] ERROR - %s\n", collection, err)
		}
	}
	for _, collection := range config.Collections {
		idx, err := openFullTextIndex(ctx, cfg, collection, false)
		if err != nil {
			return nil, stacktrace.Propagate(err, "")
		}
		c := &Collection{
			schema: collection,
			core:   store.Core(kv, idx, d.machine),
			db:     d,
		}
		for _, m := range config.middlewares {
			c.core = c.core.Apply(m)
		}
		d.collections = append(d.collections, c)
		d.closers = append(d.closers, func() error {
			if idx != nil {
				return idx.Close()
			}
			return nil
		})
	}
	{
		systemCollection, err := schema.LoadCollection(systemCollectionSchema)
		if err != nil {
			return nil, stacktrace.Propagate(err, "")
		}
		idx, err := openFullTextIndex(ctx, cfg, systemCollection, false)
		if err != nil {
			return nil, stacktrace.Propagate(err, "")
		}
		c := &Collection{
			schema: systemCollection,
			core:   store.Core(kv, idx, d.machine),
			db:     d,
		}
		for _, m := range config.middlewares {
			c.core = c.core.Apply(m)
		}
		d.collections = append(d.collections, c)
		d.closers = append(d.closers, func() error {
			if idx != nil {
				return idx.Close()
			}
			return nil
		})
	}

	if err := d.ReIndex(ctx); err != nil {
		return nil, stacktrace.Propagate(err, "failed to reindex")
	}
	d.closers = append(d.closers, func() error {
		if err := d.kv.Sync(); err != nil {
			return stacktrace.Propagate(err, "")
		}
		return stacktrace.Propagate(d.kv.Close(), "")
	})
	return d, nil
}

func (d *DB) Close(ctx context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, c := range d.closers {
		if err := c(); err != nil {
			return stacktrace.Propagate(err, "")
		}
	}
	return nil
}

func (d *DB) Backup(ctx context.Context, w io.Writer) error {
	_, err := d.kv.Backup(w, 0)
	if err != nil {
		return stacktrace.Propagate(err, "failed backup")
	}
	return nil
}

func (d *DB) Restore(ctx context.Context, r io.Reader) error {
	if err := d.kv.Load(r, 256); err != nil {
		return stacktrace.Propagate(err, "")
	}
	return stacktrace.Propagate(d.ReIndex(ctx), "")
}

func (d *DB) ReIndex(ctx context.Context) error {
	egp, ctx := errgroup.WithContext(ctx)
	for _, c := range d.collections {
		c := c
		egp.Go(func() error {
			return d.Collection(ctx, c.schema.Collection(), func(c *Collection) error {
				return c.Reindex(ctx)
			})
		})
	}
	return stacktrace.Propagate(egp.Wait(), "")
}

func (d *DB) Collection(ctx context.Context, collection string, fn func(collection *Collection) error) error {
	d.mu.RLock()
	defer d.mu.RUnlock()
	for _, c := range d.collections {
		if c.schema.Collection() == collection {
			return fn(c)
		}
	}
	return stacktrace.NewError("collection not found")
}

//go:embed system.json
var systemCollectionSchema string

func openFullTextIndex(ctx context.Context, config Config, schema *schema.Collection, reindex bool) (bleve.Index, error) {
	if !schema.Indexing().HasSearchIndex() {
		return nil, nil
	}
	i := schema.Indexing().Search[0]
	documentMapping := bleve.NewDocumentMapping()
	for _, f := range i.Fields {
		mapping := bleve.NewTextFieldMapping()
		documentMapping.AddFieldMappingsAt(f, mapping)
	}

	indexMapping := bleve.NewIndexMapping()
	indexMapping.AddDocumentMapping(schema.Collection(), documentMapping)

	path := fmt.Sprintf("%s/search/%s/index.db", config.StoragePath, schema.Collection())
	if reindex {
		os.RemoveAll(path)
	}
	switch {
	case config.StoragePath == "" && !reindex:
		i, err := bleve.NewMemOnly(indexMapping)
		if err != nil {
			return nil, stacktrace.Propagate(err, "failed to create %s search index", schema.Collection())
		}
		return i, nil
	case config.StoragePath == "" && reindex:
		i, err := bleve.NewMemOnly(indexMapping)
		if err != nil {
			return nil, stacktrace.Propagate(err, "failed to create %s search index", schema.Collection())
		}
		return i, nil
	case reindex && config.StoragePath != "":
		i, err := bleve.New(path, indexMapping)
		if err != nil {
			return nil, stacktrace.Propagate(err, "failed to create %s search index at path: %s", schema.Collection(), path)
		}
		return i, nil
	default:
		i, err := bleve.Open(path)
		if err == nil {
			return i, nil
		} else {
			i, err = bleve.New(path, indexMapping)
			if err != nil {
				return nil, stacktrace.Propagate(err, "failed to create %s search index at path: %s", schema.Collection(), path)
			}
			return i, nil
		}
	}
}
