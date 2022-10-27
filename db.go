package wolverine

import (
	"context"
	_ "embed"
	"github.com/autom8ter/machine/v4"
	"github.com/autom8ter/wolverine/core"
	"github.com/autom8ter/wolverine/errors"
	"github.com/autom8ter/wolverine/internal/coreimp"
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
	Collections []*core.Collection `json:"collections"`
	// Middlewares are middlewares to apply to the database instance
	Middlewares []core.Middleware
}

// LoadConfig loads a config instance from the specified storeage path and a directory containing the collection schemas
func LoadConfig(storeagePath string, schemaDir string, middlewares ...core.Middleware) (Config, error) {
	collections, err := core.LoadCollectionsFromDir(schemaDir)
	if err != nil {
		return Config{}, stacktrace.Propagate(err, "")
	}
	return Config{
		StoragePath: storeagePath,
		Collections: collections,
		Middlewares: middlewares,
	}, nil
}

// DB is an embedded, durable NoSQL database with support for schemas, full text search, and aggregation
type DB struct {
	config          Config
	core            core.Core
	machine         machine.Machine
	collections     []*Collection
	collectionNames []string
}

// NewFromCore creates a DB instance from the given core API. This function should only be used if the underlying database
// engine needs to be swapped out.
func NewFromCore(ctx context.Context, cfg Config, c core.Core) (*DB, error) {
	if len(cfg.Collections) == 0 {
		return nil, stacktrace.NewErrorWithCode(errors.ErrTODO, "zero collections configured")
	}
	d := &DB{
		config:  cfg,
		core:    c,
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

// New creates a new database instance from the given config
func New(ctx context.Context, cfg Config) (*DB, error) {
	rcore, err := coreimp.Default(cfg.StoragePath, cfg.Collections, cfg.Middlewares...)
	if err != nil {
		return nil, stacktrace.NewErrorWithCode(errors.ErrTODO, "failed to configure core provider")
	}
	return NewFromCore(ctx, cfg, rcore)
}

// Close closes the database
func (d *DB) Close(ctx context.Context) error {
	return d.core.Close(ctx)
}

// Backup backs up the database to the given writer
func (d *DB) Backup(ctx context.Context, w io.Writer) error {
	return d.core.Backup(ctx, w, 0)
}

// Restore restores data from the given reader
func (d *DB) Restore(ctx context.Context, r io.Reader) error {
	if err := d.core.Restore(ctx, r); err != nil {
		return stacktrace.Propagate(err, "")
	}
	return stacktrace.Propagate(d.ReIndex(ctx), "")
}

// ReIndex reindexes every collection in the database
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

// Collection executes the given function on the collection
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
