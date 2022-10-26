package wolverine

import (
	"context"
	_ "embed"
	"github.com/autom8ter/machine/v4"
	"github.com/autom8ter/wolverine/core"
	"github.com/autom8ter/wolverine/errors"
	"github.com/autom8ter/wolverine/internal/coreimp"
	"github.com/autom8ter/wolverine/schema"
	"github.com/palantir/stacktrace"
	"github.com/samber/lo"
	"golang.org/x/sync/errgroup"
	"io"
	"sort"
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
	config          Config
	core            core.Core
	machine         machine.Machine
	collections     []*Collection
	collectionNames []string
}

func New(ctx context.Context, cfg Config) (*DB, error) {
	if len(cfg.Collections) == 0 {
		return nil, stacktrace.NewErrorWithCode(errors.ErrTODO, "zero collections configured")
	}
	rcore, err := coreimp.Default(cfg.StoragePath, cfg.Collections, cfg.middlewares...)
	if err != nil {
		return nil, stacktrace.NewErrorWithCode(errors.ErrTODO, "failed to configure core provider")
	}
	d := &DB{
		config:  cfg,
		core:    rcore,
		machine: machine.New(),
	}

	for _, collection := range cfg.Collections {
		d.collections = append(d.collections, &Collection{
			schema: collection,
			db:     d,
		})
		d.collectionNames = append(d.collectionNames, collection.Collection())
	}
	if err := d.ReIndex(ctx); err != nil {
		return nil, stacktrace.Propagate(err, "failed to reindex")
	}
	sort.Strings(d.collectionNames)
	return d, nil
}

func (d *DB) Close(ctx context.Context) error {
	return d.core.Close(ctx)
}

func (d *DB) Backup(ctx context.Context, w io.Writer) error {
	return d.core.Backup(ctx, w, 0)
}

func (d *DB) Restore(ctx context.Context, r io.Reader) error {
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
	for _, c := range d.collections {
		if c.schema.Collection() == collection {
			return fn(c)
		}
	}
	return stacktrace.NewError("collection not found")
}

// Collections executes the given function on each registered collection concurrently then waits for all functions to finish executing
func (d *DB) Collections(ctx context.Context, fn func(collection *Collection) error) error {
	egp, ctx := errgroup.WithContext(ctx)
	for _, c := range d.collections {
		c := c
		egp.Go(func() error {
			return stacktrace.Propagate(fn(c), "")
		})
	}
	return egp.Wait()
}

// RegisteredCollections returns a list of registered collections
func (d *DB) RegisteredCollections() []string {
	return d.collectionNames
}

// HasCollection returns true if the collection is present in the database
func (d *DB) HasCollection(name string) bool {
	return lo.Contains(d.collectionNames, name)
}
