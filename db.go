package wolverine

import (
	"context"
	_ "embed"
	"github.com/autom8ter/machine/v4"
	"github.com/autom8ter/wolverine/core"
	"github.com/autom8ter/wolverine/errors"
	"github.com/autom8ter/wolverine/runtime"
	"github.com/autom8ter/wolverine/schema"
	"github.com/palantir/stacktrace"
	"golang.org/x/sync/errgroup"
	"io"
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

// AddMiddleware adds a middleware to the configuration. 0-many middlewares are supported
func (c Config) AddMiddleware(m core.Middleware) Config {
	middlewares := append(c.middlewares, m)
	return Config{
		StoragePath: c.StoragePath,
		Collections: c.Collections,
		middlewares: middlewares,
	}
}

// LoadConfig loads a config instance from the specified storeage path and a directory containing the collection schemas
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
	config      Config
	mu          sync.RWMutex
	core        core.Core
	machine     machine.Machine
	collections []*Collection
}

func New(ctx context.Context, cfg Config) (*DB, error) {
	if len(cfg.Collections) == 0 {
		return nil, stacktrace.NewErrorWithCode(errors.ErrTODO, "zero collections configured")
	}
	rcore, err := runtime.Default(cfg.StoragePath, cfg.Collections, cfg.middlewares...)
	if err != nil {
		return nil, stacktrace.NewErrorWithCode(errors.ErrTODO, "failed to configure core provider")
	}
	d := &DB{
		config:  cfg,
		mu:      sync.RWMutex{},
		core:    rcore,
		machine: machine.New(),
	}
	for _, collection := range cfg.Collections {
		d.collections = append(d.collections, &Collection{
			schema: collection,
			db:     d,
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
	return d.core.Close(ctx)
}

func (d *DB) Backup(ctx context.Context, w io.Writer) error {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.core.Backup(ctx, w, 0)
}

func (d *DB) Restore(ctx context.Context, r io.Reader) error {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if err := d.core.Restore(ctx, r); err != nil {
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

// Core returns the underlying core implementation
func (d *DB) Core() core.Core {
	return d.core
}
