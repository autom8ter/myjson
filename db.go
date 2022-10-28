package wolverine

import (
	"context"
	_ "embed"
	"github.com/autom8ter/machine/v4"
	"github.com/autom8ter/wolverine/core"
	"github.com/autom8ter/wolverine/errors"
	"github.com/autom8ter/wolverine/kv"
	"github.com/autom8ter/wolverine/kv/badger"
	"github.com/autom8ter/wolverine/middleware"
	"github.com/palantir/stacktrace"
	"github.com/samber/lo"
	"github.com/segmentio/ksuid"
	"golang.org/x/sync/errgroup"
	"sort"
	"time"
)

// Config configures a database instance
type Config struct {
	Params map[string]string `json:"params"`
	// Collections are the json document collections supported by the DB - At least one is required.
	Collections []*core.Collection `json:"collections"`
	// Middlewares are middlewares to apply to the database instance
	Middlewares []middleware.Middleware
}

// LoadConfig loads a config instance from the specified storeage path and a directory containing the collection schemas
func LoadConfig(params map[string]string, schemaDir string, middlewares ...middleware.Middleware) (Config, error) {
	collections, err := core.LoadCollectionsFromDir(schemaDir)
	if err != nil {
		return Config{}, stacktrace.Propagate(err, "")
	}
	return Config{
		Params:      params,
		Collections: collections,
		Middlewares: middlewares,
	}, nil
}

// DB is an embedded, durable NoSQL database with support for schemas, full text search, and aggregation
type DB struct {
	config          Config
	core            core.CoreAPI
	machine         machine.Machine
	collections     []*Collection
	collectionNames []string
}

// NewFromCore creates a DB instance from the given core API. This function should only be used if the underlying database
// engine needs to be swapped out.
func NewFromCore(ctx context.Context, cfg Config, c core.CoreAPI) (*DB, error) {
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
	var (
		db  kv.DB
		err error
	)
	switch cfg.Params["provider"] {
	default:
		db, err = badger.New(cfg.Params["storage_path"])
	}
	rcore, err := core.NewCore(db, cfg.Collections)
	if err != nil {
		return nil, stacktrace.NewErrorWithCode(errors.ErrTODO, "failed to configure core provider")
	}
	return NewFromCore(ctx, cfg, rcore)
}

// Close closes the database
func (d *DB) Close(ctx context.Context) error {
	return d.core.Close(ctx)
}

// ReIndex reindexes every collection in the database
func (d *DB) ReIndex(ctx context.Context) error {
	egp, ctx := errgroup.WithContext(ctx)
	for _, c := range d.collections {
		c := c
		egp.Go(func() error {
			return d.Collection(c.schema.Collection()).Reindex(ctx)
		})
	}
	return stacktrace.Propagate(egp.Wait(), "")
}

// Collection executes the given function on the collection
func (d *DB) Collection(collection string) *Collection {
	for _, c := range d.collections {
		if c.schema.Collection() == collection {
			return c
		}
	}
	return nil
}

// RegisteredCollections returns a list of registered collections
func (d *DB) RegisteredCollections() []string {
	return d.collectionNames
}

// HasCollection returns true if the collection is present in the database
func (d *DB) HasCollection(name string) bool {
	return lo.Contains(d.collectionNames, name)
}

// Collection is collection of documents in the database with the same schema. !-many collections are supported.
type Collection struct {
	schema *core.Collection
	db     *DB
}

// DB returns the collections underlying database connection
func (c *Collection) DB() *DB {
	return c.db
}

// Schema returns the collecctions schema information
func (c *Collection) Schema() *core.Collection {
	return c.schema
}

func (c *Collection) persistStateChange(ctx context.Context, change core.StateChange) error {
	return c.db.core.Persist(ctx, c.schema, change)
}

// Query queries a list of documents
func (c *Collection) Query(ctx context.Context, query core.Query) (core.Page, error) {
	return c.db.core.Query(ctx, c.schema, query)
}

// Get gets a single document by id
func (c *Collection) Get(ctx context.Context, id string) (*core.Document, error) {
	var doc *core.Document
	if err := c.db.core.Scan(ctx, c.schema, core.Scan{
		Filter: []core.Where{
			{
				Field: c.schema.PrimaryKey(),
				Op:    core.Eq,
				Value: id,
			},
		},
	}, func(d *core.Document) (bool, error) {
		doc = d
		return false, nil
	}); err != nil {
		return nil, stacktrace.Propagate(err, "")
	}
	return doc, nil
}

// GetAll gets all documents by ids
func (c *Collection) GetAll(ctx context.Context, ids []string) ([]*core.Document, error) {
	var docs = make([]*core.Document, len(ids))
	if err := c.db.core.Scan(ctx, c.schema, core.Scan{
		Filter: []core.Where{
			{
				Field: c.schema.PrimaryKey(),
				Op:    core.In,
				Value: ids,
			},
		},
	}, func(d *core.Document) (bool, error) {
		docs = append(docs, d)
		return false, nil
	}); err != nil {
		return nil, stacktrace.Propagate(err, "")
	}
	return docs, nil
}

// QueryPaginate paginates through each page of the query until the handlePage function returns false or there are no more results
func (c *Collection) QueryPaginate(ctx context.Context, query core.Query, handlePage core.PageHandler) error {
	page := query.Page
	for {
		results, err := c.Query(ctx, core.Query{
			Select:  query.Select,
			Where:   query.Where,
			Page:    page,
			Limit:   query.Limit,
			OrderBy: query.OrderBy,
		})
		if err != nil {
			return stacktrace.Propagate(err, "failed to query collection: %s", c.schema.Collection())
		}
		if len(results.Documents) == 0 {
			return nil
		}
		if !handlePage(results) {
			return nil
		}
		page++
	}
}

// ChangeStream streams all state changes to the given function
func (c *Collection) ChangeStream(ctx context.Context, fn core.ChangeStreamHandler) error {
	return c.db.core.ChangeStream(ctx, c.schema, fn)
}

// Create creates a new document - if the documents primary key is unset, it will be set as a sortable unique id
func (c *Collection) Create(ctx context.Context, document *core.Document) (string, error) {
	if c.schema.GetPrimaryKey(document) == "" {
		id := ksuid.New().String()
		err := c.schema.SetPrimaryKey(document, id)
		if err != nil {
			return "", stacktrace.Propagate(err, "")
		}
	}
	return c.schema.GetPrimaryKey(document), stacktrace.Propagate(c.persistStateChange(ctx, core.StateChange{
		Collection: c.schema.Collection(),
		Sets:       []*core.Document{document},
		Timestamp:  time.Now(),
	}), "")
}

// BatchCreate creates 1-many documents. If each documents primary key is unset, it will be set as a sortable unique id.
func (c *Collection) BatchCreate(ctx context.Context, documents []*core.Document) ([]string, error) {
	var ids []string
	for _, document := range documents {
		if c.schema.GetPrimaryKey(document) == "" {
			id := ksuid.New().String()
			err := c.schema.SetPrimaryKey(document, id)
			if err != nil {
				return nil, stacktrace.Propagate(err, "")
			}
		}
		ids = append(ids, c.schema.GetPrimaryKey(document))
	}

	if err := c.persistStateChange(ctx, core.StateChange{
		Collection: c.schema.Collection(),
		Sets:       documents,
		Timestamp:  time.Now(),
	}); err != nil {
		return nil, stacktrace.Propagate(err, "")
	}
	return ids, nil
}

// Set overwrites a single document. The documents primary key must be set.
func (c *Collection) Set(ctx context.Context, document *core.Document) error {
	return stacktrace.Propagate(c.persistStateChange(ctx, core.StateChange{
		Collection: c.schema.Collection(),
		Sets:       []*core.Document{document},
		Timestamp:  time.Now(),
	}), "")
}

// BatchSet overwrites 1-many documents. The documents primary key must be set.
func (c *Collection) BatchSet(ctx context.Context, batch []*core.Document) error {
	return stacktrace.Propagate(c.persistStateChange(ctx, core.StateChange{
		Collection: c.schema.Collection(),
		Sets:       batch,
		Timestamp:  time.Now(),
	}), "")
}

// Update patches a single document. The documents primary key must be set.
func (c *Collection) Update(ctx context.Context, id string, update map[string]any) error {
	return stacktrace.Propagate(c.persistStateChange(ctx, core.StateChange{
		Collection: c.schema.Collection(),
		Updates: map[string]map[string]any{
			id: update,
		},
		Timestamp: time.Now(),
	}), "")
}

// BatchUpdate patches a 1-many documents. The documents primary key must be set.
func (c *Collection) BatchUpdate(ctx context.Context, batch map[string]map[string]any) error {
	return stacktrace.Propagate(c.persistStateChange(ctx, core.StateChange{
		Collection: c.schema.Collection(),
		Updates:    batch,
		Timestamp:  time.Now(),
	}), "")
}

// Delete deletes a single document by id
func (c *Collection) Delete(ctx context.Context, id string) error {
	return stacktrace.Propagate(c.persistStateChange(ctx, core.StateChange{
		Collection: c.schema.Collection(),
		Deletes:    []string{id},
		Timestamp:  time.Now(),
	}), "")
}

// BatchDelete deletes 1-many documents by id
func (c *Collection) BatchDelete(ctx context.Context, ids []string) error {
	return stacktrace.Propagate(c.persistStateChange(ctx, core.StateChange{
		Collection: c.schema.Collection(),
		Deletes:    ids,
		Timestamp:  time.Now(),
	}), "")
}

// QueryUpdate updates the documents returned from the query
func (c *Collection) QueryUpdate(ctx context.Context, update map[string]any, query core.Query) error {
	results, err := c.Query(ctx, query)
	if err != nil {
		return stacktrace.Propagate(err, "")
	}
	for _, document := range results.Documents {
		err := document.SetAll(update)
		if err != nil {
			return stacktrace.Propagate(err, "")
		}
	}
	return stacktrace.Propagate(c.BatchSet(ctx, results.Documents), "")
}

// QueryDelete deletes the documents returned from the query
func (c *Collection) QueryDelete(ctx context.Context, query core.Query) error {
	results, err := c.Query(ctx, query)
	if err != nil {
		return stacktrace.Propagate(err, "")
	}
	var ids []string
	for _, document := range results.Documents {
		ids = append(ids, c.schema.GetPrimaryKey(document))
	}
	return stacktrace.Propagate(c.BatchDelete(ctx, ids), "")
}

// Aggregate performs aggregations against the collection
func (c *Collection) Aggregate(ctx context.Context, query core.AggregateQuery) (core.Page, error) {
	return c.db.core.Aggregate(ctx, c.schema, query)
}

// Reindex the collection
func (c *Collection) Reindex(ctx context.Context) error {
	meta, ok := core.GetContext(ctx)
	if !ok {
		meta = core.NewContext(map[string]any{})
	}
	meta.Set("_reindexing", true)
	meta.Set("_internal", true)
	egp, ctx := errgroup.WithContext(meta.ToContext(ctx))
	var page int
	for {

		results, err := c.Query(ctx, core.Query{
			Select:  nil,
			Where:   nil,
			Page:    page,
			Limit:   1000,
			OrderBy: core.OrderBy{},
		})
		if err != nil {
			return stacktrace.Propagate(err, "failed to reindex collection: %s", c.schema.Collection())
		}
		if len(results.Documents) == 0 {
			break
		}
		var toSet []*core.Document
		var toDelete []string
		for _, r := range results.Documents {
			result, _ := c.Get(ctx, c.schema.GetPrimaryKey(r))
			if result.Valid() {
				toSet = append(toSet, result)
			} else {
				toDelete = append(toDelete, c.schema.GetPrimaryKey(r))
				_ = c.Delete(ctx, c.schema.GetPrimaryKey(r))
			}
		}
		if len(toSet) > 0 {
			egp.Go(func() error {
				return stacktrace.Propagate(c.BatchSet(ctx, toSet), "")
			})
		}
		if len(toDelete) > 0 {
			egp.Go(func() error {
				return stacktrace.Propagate(c.BatchDelete(ctx, toDelete), "")
			})
		}
		page = results.NextPage
	}
	if err := egp.Wait(); err != nil {
		return stacktrace.Propagate(err, "failed to reindex collection: %s", c.schema.Collection())
	}
	return nil
}

// Transform executes a transformation which is basically ETL from one collection to another
func (c *Collection) Transform(ctx context.Context, transformation core.ETL) error {
	if transformation.Transformer == nil {
		return stacktrace.NewError("empty transformer")
	}
	if transformation.OutputCollection == "" {
		return stacktrace.NewError("empty output collection")
	}
	res, err := c.Query(ctx, transformation.Query)
	if err != nil {
		return stacktrace.Propagate(err, "")
	}
	res.Documents, err = transformation.Transformer(ctx, res.Documents)
	if err != nil {
		return stacktrace.Propagate(err, "")
	}
	if len(res.Documents) > 0 {
		if err := c.db.Collection(transformation.OutputCollection).BatchSet(ctx, res.Documents); err != nil {
			return stacktrace.Propagate(err, "")
		}
	}
	return nil
}
