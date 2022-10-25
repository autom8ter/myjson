package wolverine

import (
	"context"
	_ "embed"
	"fmt"
	"github.com/autom8ter/machine/v4"
	"github.com/autom8ter/wolverine/errors"
	"github.com/autom8ter/wolverine/schema"
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
}

type DB struct {
	config       Config
	kv           *badger.DB
	mu           sync.RWMutex
	schema       *schema.Schema
	machine      machine.Machine
	collections  []*Collection
	errorHandler func(collection string, err error)
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
		c := &Collection{
			schema:       collection,
			kv:           d.kv,
			triggers:     nil,
			machine:      d.machine,
			errorHandler: d.errorHandler,
			db:           d,
		}
		if err := c.openFullTextIndex(ctx, false); err != nil {
			return nil, stacktrace.Propagate(err, "")
		}
		d.collections = append(d.collections, c)
	}
	{
		systemCollection, err := schema.LoadCollection(systemCollectionSchema)
		if err != nil {
			return nil, stacktrace.Propagate(err, "")
		}
		c := &Collection{
			schema:       systemCollection,
			kv:           d.kv,
			triggers:     nil,
			machine:      d.machine,
			errorHandler: d.errorHandler,
			db:           d,
		}
		if err := c.openFullTextIndex(ctx, false); err != nil {
			return nil, stacktrace.Propagate(err, "")
		}
		d.collections = append(d.collections, c)
	}

	if err := d.ReIndex(ctx); err != nil {
		return nil, stacktrace.Propagate(err, "failed to reindex")
	}
	return d, nil
}

func (d *DB) Close(ctx context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	egp, ctx := errgroup.WithContext(ctx)
	for _, c := range d.collections {
		c := c
		egp.Go(func() error {
			return c.Close(ctx)
		})
	}
	return stacktrace.Propagate(egp.Wait(), "")
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

func (c *Collection) openFullTextIndex(ctx context.Context, reindex bool) error {
	if !c.schema.Indexing().HasSearchIndex() {
		return nil
	}
	i := c.schema.Indexing().Search[0]
	documentMapping := bleve.NewDocumentMapping()
	for _, f := range i.Fields {
		mapping := bleve.NewTextFieldMapping()
		documentMapping.AddFieldMappingsAt(f, mapping)
	}

	indexMapping := bleve.NewIndexMapping()
	indexMapping.AddDocumentMapping(c.schema.Collection(), documentMapping)
	indexMapping.TypeField = "_collection"

	path := fmt.Sprintf("%s/search/%s/index.db", c.db.config.StoragePath, c.schema.Collection())
	if reindex {
		os.RemoveAll(path)
	}
	switch {
	case c.db.config.StoragePath == "" && !reindex:
		i, err := bleve.NewMemOnly(indexMapping)
		if err != nil {
			return stacktrace.Propagate(err, "failed to create %s search index", c.schema.Collection())
		}
		c.fullText = i
	case c.db.config.StoragePath == "" && reindex:
		i, err := bleve.NewMemOnly(indexMapping)
		if err != nil {
			return stacktrace.Propagate(err, "failed to create %s search index", c.schema.Collection())
		}
		c.fullText = i
	case reindex && c.db.config.StoragePath != "":
		i, err := bleve.New(path, indexMapping)
		if err != nil {
			return stacktrace.Propagate(err, "failed to create %s search index at path: %s", c.schema.Collection(), path)
		}
		c.fullText = i
	default:
		i, err := bleve.Open(path)
		if err == nil {
			c.fullText = i
		} else {
			i, err = bleve.New(path, indexMapping)
			if err != nil {
				return stacktrace.Propagate(err, "failed to create %s search index at path: %s", c.schema.Collection(), path)
			}
			c.fullText = i
		}
	}
	return nil
}
