package wolverine

import (
	"context"
	"github.com/autom8ter/wolverine/core"
	"github.com/palantir/stacktrace"
	"github.com/segmentio/ksuid"
	"golang.org/x/sync/errgroup"
	"time"
)

type Collection struct {
	schema *core.Collection
	db     *DB
}

// DB returns the collections underlying database connection
func (c *Collection) DB() *DB {
	return c.db
}

// Schema
func (c *Collection) Schema() *core.Collection {
	return c.schema
}

func (c *Collection) persistStateChange(ctx context.Context, change core.StateChange) error {
	return c.db.core.Persist(ctx, c.schema, change)
}

// Query
func (c *Collection) Query(ctx context.Context, query core.Query) (core.Page, error) {
	return c.db.core.Query(ctx, c.schema, query)
}

// Get
func (c *Collection) Get(ctx context.Context, id string) (*core.Document, error) {
	return c.db.core.Get(ctx, c.schema, id)
}

// GetAll
func (c *Collection) GetAll(ctx context.Context, ids []string) ([]*core.Document, error) {
	return c.db.core.GetAll(ctx, c.schema, ids)
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

// ChangeStream
func (c *Collection) ChangeStream(ctx context.Context, fn core.ChangeStreamHandler) error {
	return c.db.core.ChangeStream(ctx, c.schema, fn)
}

// Create
func (c *Collection) Create(ctx context.Context, document *core.Document) (string, error) {
	id := ksuid.New().String()
	err := document.Set(c.schema.PKey(), id)
	if err != nil {
		return "", stacktrace.Propagate(err, "")
	}
	return id, stacktrace.Propagate(c.persistStateChange(ctx, core.StateChange{
		Collection: c.schema.Collection(),
		Sets:       []*core.Document{document},
		Timestamp:  time.Now(),
	}), "")
}

// Create
func (c *Collection) BatchCreate(ctx context.Context, documents []*core.Document) ([]string, error) {
	var ids []string
	for _, document := range documents {
		id := ksuid.New().String()
		err := document.Set(c.schema.PKey(), id)
		if err != nil {
			return nil, stacktrace.Propagate(err, "")
		}
		ids = append(ids, id)
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

// Set
func (c *Collection) Set(ctx context.Context, document *core.Document) error {
	return stacktrace.Propagate(c.persistStateChange(ctx, core.StateChange{
		Collection: c.schema.Collection(),
		Sets:       []*core.Document{document},
		Timestamp:  time.Now(),
	}), "")
}

// BatchSet
func (c *Collection) BatchSet(ctx context.Context, batch []*core.Document) error {
	return stacktrace.Propagate(c.persistStateChange(ctx, core.StateChange{
		Collection: c.schema.Collection(),
		Sets:       batch,
		Timestamp:  time.Now(),
	}), "")
}

// Update
func (c *Collection) Update(ctx context.Context, id string, update map[string]any) error {
	return stacktrace.Propagate(c.persistStateChange(ctx, core.StateChange{
		Collection: c.schema.Collection(),
		Updates: map[string]map[string]any{
			id: update,
		},
		Timestamp: time.Now(),
	}), "")
}

// BatchDelete
func (c *Collection) BatchUpdate(ctx context.Context, batch map[string]map[string]any) error {
	return stacktrace.Propagate(c.persistStateChange(ctx, core.StateChange{
		Collection: c.schema.Collection(),
		Updates:    batch,
		Timestamp:  time.Now(),
	}), "")
}

// Delete
func (c *Collection) Delete(ctx context.Context, id string) error {
	return stacktrace.Propagate(c.persistStateChange(ctx, core.StateChange{
		Collection: c.schema.Collection(),
		Deletes:    []string{id},
		Timestamp:  time.Now(),
	}), "")
}

// BatchDelete
func (c *Collection) BatchDelete(ctx context.Context, ids []string) error {
	return stacktrace.Propagate(c.persistStateChange(ctx, core.StateChange{
		Collection: c.schema.Collection(),
		Deletes:    ids,
		Timestamp:  time.Now(),
	}), "")
}

// QueryUpdate
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

// QueryDelete
func (c *Collection) QueryDelete(ctx context.Context, query core.Query) error {
	results, err := c.Query(ctx, query)
	if err != nil {
		return stacktrace.Propagate(err, "")
	}
	var ids []string
	for _, document := range results.Documents {
		ids = append(ids, c.schema.GetDocumentID(document))
	}
	return stacktrace.Propagate(c.BatchDelete(ctx, ids), "")
}

// Aggregate
func (c *Collection) Aggregate(ctx context.Context, query core.AggregateQuery) (core.Page, error) {
	return c.db.core.Aggregate(ctx, c.schema, query)
}

// Search
func (c *Collection) Search(ctx context.Context, query core.SearchQuery) (core.Page, error) {
	return c.db.core.Search(ctx, c.schema, query)
}

// SearchPaginate paginates through each page of the query until the handlePage function returns false or there are no more results
func (c *Collection) SearchPaginate(ctx context.Context, query core.SearchQuery, handlePage core.PageHandler) error {
	page := query.Page
	for {
		results, err := c.Search(ctx, core.SearchQuery{
			Select: query.Select,
			Where:  query.Where,
			Page:   page,
			Limit:  query.Limit,
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
		page = results.NextPage
	}
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
			result, _ := c.Get(ctx, c.schema.GetDocumentID(r))
			if result.Valid() {
				toSet = append(toSet, result)
			} else {
				toDelete = append(toDelete, c.schema.GetDocumentID(r))
				_ = c.Delete(ctx, c.schema.GetDocumentID(r))
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
		if err := c.db.Collection(ctx, transformation.OutputCollection, func(dest *Collection) error {
			return stacktrace.Propagate(dest.BatchSet(ctx, res.Documents), "")
		}); err != nil {
			return stacktrace.Propagate(err, "")
		}
	}
	return nil
}
