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
	"io/fs"
	"os"
	"sort"
	"sync"
	"time"
)

// Config configures a database instance
type Config struct {
	// StoragePath is the path to database storage.
	// Leave empty to operate the database in memory only.
	StoragePath string `json:"storagePath"`
	// Collections are the json document collections supported by the DB - At least one is required.
	Collections []*schema.Collection `json:"collections"`
	// ErrorHandler, if set, handles database errors - ex: log to stderr
	ErrorHandler func(collection string, err error)
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
		config:       *config,
		kv:           kv,
		mu:           sync.RWMutex{},
		schema:       schema.NewSchema(nil),
		machine:      machine.New(),
		errorHandler: config.ErrorHandler,
	}
	if d.errorHandler == nil {
		d.errorHandler = func(collection string, err error) {
			fmt.Printf("[%s] ERROR - %s\n", collection, err)
		}
	}
	for _, collection := range config.Collections {
		ft, err := openFullTextIndex(*config, collection.Collection(), false)
		if err != nil {
			return nil, stacktrace.Propagate(err, "")
		}
		d.collections = append(d.collections, &Collection{
			schema:       collection,
			kv:           d.kv,
			fullText:     ft,
			triggers:     nil,
			machine:      d.machine,
			errorHandler: d.errorHandler,
			db:           d,
		})
	}
	{
		systemCollection, err := schema.LoadCollection(systemCollectionSchema)
		if err != nil {
			return nil, stacktrace.Propagate(err, "")
		}
		ft, err := openFullTextIndex(*config, systemCollection.Collection(), false)
		if err != nil {
			return nil, stacktrace.Propagate(err, "")
		}
		d.collections = append(d.collections, &Collection{
			schema:       systemCollection,
			kv:           d.kv,
			fullText:     ft,
			triggers:     nil,
			machine:      d.machine,
			errorHandler: d.errorHandler,
			db:           d,
		})
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

func openFullTextIndex(config Config, collection string, reindex bool) (bleve.Index, error) {
	indexMapping := bleve.NewIndexMapping()
	newPath := fmt.Sprintf("%s/search/%s/index_%v.db", config.StoragePath, collection, time.Now().Unix())
	switch {
	case config.StoragePath == "" && !reindex:
		i, err := bleve.NewMemOnly(indexMapping)
		if err != nil {
			return nil, stacktrace.Propagate(err, "failed to create %s search index", collection)
		}
		return i, nil
	case config.StoragePath == "" && reindex:
		i, err := bleve.NewMemOnly(indexMapping)
		if err != nil {
			return nil, stacktrace.Propagate(err, "failed to create %s search index", collection)
		}
		return i, nil
	case reindex && config.StoragePath != "":
		lastPath := getLastFullTextIndexPath(config, collection)
		i, err := bleve.New(newPath, indexMapping)
		if err != nil {
			return nil, stacktrace.Propagate(err, "failed to create %s search index at path: %s", collection, newPath)
		}
		if lastPath != "" {
			os.RemoveAll(lastPath)
		}
		return i, nil
	default:
		lastPath := getLastFullTextIndexPath(config, collection)
		i, err := bleve.Open(lastPath)
		if err == nil {
			return i, nil
		} else {
			i, err = bleve.New(newPath, indexMapping)
			if err != nil {
				return nil, stacktrace.Propagate(err, "failed to create %s search index at path: %s", collection, newPath)
			}
			return i, nil
		}
	}
}

func getLastFullTextIndexPath(config Config, collection string) string {
	fileSystem := os.DirFS(fmt.Sprintf("%s/search/%s", config.StoragePath, collection))
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
