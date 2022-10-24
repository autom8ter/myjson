package wolverine

import (
	"context"
	_ "embed"
	"fmt"
	"github.com/autom8ter/machine/v4"
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
	// Path is the path to database storage. Use 'inmem' to operate the database in memory only.
	Path string
	// Debug sets the database to debug level
	Debug bool
	// Migrate, if a true, has the database run any migrations that have not already run(idempotent).
	Migrate bool
	// ReIndex reindexes the database
	ReIndex     bool
	Collections []*schema.Collection
}

type DB struct {
	config      Config
	kv          *badger.DB
	mu          sync.RWMutex
	schema      *schema.Schema
	machine     machine.Machine
	collections []*Collection
}

func New(ctx context.Context, cfg Config) (*DB, error) {
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

	d := &DB{
		config:  *config,
		kv:      kv,
		mu:      sync.RWMutex{},
		schema:  schema.NewSchema(nil),
		machine: machine.New(),
	}
	for _, collection := range config.Collections {
		ft, err := openFullTextIndex(*config, collection.Collection(), false)
		if err != nil {
			return nil, stacktrace.Propagate(err, "")
		}
		d.collections = append(d.collections, &Collection{
			schema:   collection,
			kv:       d.kv,
			fullText: ft,
			triggers: nil,
			machine:  d.machine,
		})
	}
	{
		systemCollection, err := schema.LoadCollection(systemCollectionSchema)
		if err != nil {
			return nil, stacktrace.Propagate(err, "")
		}
		ft, err := openFullTextIndex(*config, systemCollection.Collection(), config.ReIndex)
		if err != nil {
			return nil, stacktrace.Propagate(err, "")
		}
		d.collections = append(d.collections, &Collection{
			schema:   systemCollection,
			kv:       d.kv,
			fullText: ft,
			triggers: nil,
			machine:  d.machine,
		})
	}

	if config.ReIndex {
		if err := d.ReIndex(ctx); err != nil {
			return nil, stacktrace.Propagate(err, "failed to reindex")
		}
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
	d.mu.Lock()
	defer d.mu.Unlock()
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
	indexMapping.TypeField = "_collection"
	newPath := fmt.Sprintf("%s/search/%s/index_%v.db", config.Path, collection, time.Now().Unix())
	switch {
	case config.Path == "inmem" && !reindex:
		i, err := bleve.NewMemOnly(indexMapping)
		if err != nil {
			return nil, stacktrace.Propagate(err, "failed to create %s search index", collection)
		}
		return i, nil
	case config.Path == "inmem" && reindex:
		i, err := bleve.NewMemOnly(indexMapping)
		if err != nil {
			return nil, stacktrace.Propagate(err, "failed to create %s search index", collection)
		}
		return i, nil
	case reindex && config.Path != "inmem":
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
	fileSystem := os.DirFS(fmt.Sprintf("%s/search/%s", config.Path, collection))
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
