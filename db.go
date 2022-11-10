package brutus

import (
	"context"
	_ "embed"
	"github.com/autom8ter/brutus/kv"
	"github.com/autom8ter/brutus/kv/registry"
	"github.com/autom8ter/machine/v4"
	"github.com/palantir/stacktrace"
	"github.com/samber/lo"
	"github.com/segmentio/ksuid"
	"time"
)

type KVConfig struct {
	// Provider is the name of the kv provider (badger)
	Provider string `json:"provider"`
	// Params are the kv providers params
	Params map[string]any `json:"params"`
}

// Config configures a database instance
type Config struct {
	KV KVConfig `json:"kv"`
	// Collections are the json document collections supported by the DB - At least one is required.
	Collections []*Collection `json:"collections"`
}

// DB is an embedded, durable NoSQL database with support for schemas, full text search, and aggregation
type DB struct {
	config  Config
	core    CoreAPI
	machine machine.Machine
}

// NewFromCore creates a DB instance from the given core API. This function should only be used if the underlying database
// engine needs to be swapped out.
func NewFromCore(ctx context.Context, c CoreAPI) (*DB, error) {
	d := &DB{
		core:    c,
		machine: machine.New(),
	}
	return d, nil
}

/*
OpenKV opens a kv database. supported providers:
badger(default):

	  params:
		storage_path: string (leave empty for in-memory)
*/
func OpenKV(cfg KVConfig) (kv.DB, error) {
	return registry.Open(cfg.Provider, cfg.Params)
}

// New creates a new database instance from the given config
func New(ctx context.Context, cfg KVConfig) (*DB, error) {
	db, err := OpenKV(cfg)
	if err != nil {
		return nil, stacktrace.PropagateWithCode(err, ErrTODO, "failed to open kv database")
	}
	rcore, err := NewCore(db)
	if err != nil {
		return nil, stacktrace.PropagateWithCode(err, ErrTODO, "failed to configure core provider")
	}
	return NewFromCore(ctx, rcore)
}

// Core returns the CoreAPI instance powering the database
func (d *DB) Core() CoreAPI {
	return d.core
}

// Close closes the database
func (d *DB) Close(ctx context.Context) error {
	return d.core.Close(ctx)
}

func (d *DB) persistStateChange(ctx context.Context, change StateChange) error {
	return d.core.Persist(ctx, change.Collection, change)
}

// Get gets a single document by id
func (d *DB) Get(ctx context.Context, collection, id string) (*Document, error) {
	var doc *Document
	collect, ok := d.core.GetCollection(ctx, collection)
	if !ok {
		return nil, stacktrace.NewError("unsupported collection: %s", collection)
	}
	match, err := d.core.Scan(ctx, collection, Scan{
		Filter: []Where{
			{
				Field: collect.PrimaryKey(),
				Op:    Eq,
				Value: id,
			},
		},
	}, func(d *Document) (bool, error) {
		if doc == nil {
			doc = d
		}
		return false, nil
	})
	if err != nil {
		return nil, stacktrace.Propagate(err, "%#v", match)
	}
	if doc == nil {
		return nil, stacktrace.NewErrorWithCode(ErrTODO, "%s not found", id)
	}
	if len(match.MatchedFields) == 0 || match.MatchedFields[0] != collect.PrimaryKey() {
		return nil, stacktrace.NewErrorWithCode(ErrTODO, "%s failed to get from primary index", id)
	}
	return doc, nil
}

// QueryPaginate paginates through each page of the query until the handlePage function returns false or there are no more results
func (d *DB) QueryPaginate(ctx context.Context, query Query, handlePage PageHandler) error {
	page := query.Page
	for {
		results, err := d.Query(ctx, Query{
			Select:  query.Select,
			Where:   query.Where,
			Page:    page,
			Limit:   query.Limit,
			OrderBy: query.OrderBy,
		})
		if err != nil {
			return stacktrace.Propagate(err, "failed to query collection: %s", query.From)
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

// Create creates a new document - if the documents primary key is unset, it will be set as a sortable unique id
func (d *DB) Create(ctx context.Context, collection string, document *Document) (string, error) {
	collect, ok := d.core.GetCollection(ctx, collection)
	if !ok {
		return "", stacktrace.NewError("unsupported collection: %s", collection)
	}
	if collect.GetPrimaryKey(document) == "" {
		id := ksuid.New().String()
		err := collect.SetPrimaryKey(document, id)
		if err != nil {
			return "", stacktrace.Propagate(err, "")
		}
	}
	return collect.GetPrimaryKey(document), stacktrace.Propagate(d.persistStateChange(ctx, StateChange{
		ctx:        nil,
		Collection: collection,
		Deletes:    nil,
		Creates:    []*Document{document},
		Sets:       nil,
		Updates:    nil,
		Timestamp:  time.Now(),
	}), "")
}

// BatchCreate creates 1-many documents. If each documents primary key is unset, it will be set as a sortable unique id.
func (d *DB) BatchCreate(ctx context.Context, collection string, documents []*Document) ([]string, error) {
	collect, ok := d.core.GetCollection(ctx, collection)
	if !ok {
		return nil, stacktrace.NewError("unsupported collection: %s", collection)
	}
	var ids []string
	for _, document := range documents {
		if collect.GetPrimaryKey(document) == "" {
			id := ksuid.New().String()
			err := collect.SetPrimaryKey(document, id)
			if err != nil {
				return nil, stacktrace.Propagate(err, "")
			}
		}
		ids = append(ids, collect.GetPrimaryKey(document))
	}

	if err := d.persistStateChange(ctx, StateChange{
		Collection: collection,
		Creates:    documents,
		Timestamp:  time.Now(),
	}); err != nil {
		return nil, stacktrace.Propagate(err, "")
	}
	return ids, nil
}

// Set overwrites a single document. The documents primary key must be set.
func (d *DB) Set(ctx context.Context, collection string, document *Document) error {
	return stacktrace.Propagate(d.persistStateChange(ctx, StateChange{
		Collection: collection,
		Sets:       []*Document{document},
		Timestamp:  time.Now(),
	}), "")
}

// BatchSet overwrites 1-many documents. The documents primary key must be set.
func (d *DB) BatchSet(ctx context.Context, collection string, batch []*Document) error {
	return stacktrace.Propagate(d.persistStateChange(ctx, StateChange{
		Collection: collection,
		Sets:       batch,
		Timestamp:  time.Now(),
	}), "")
}

// Update patches a single document. The documents primary key must be set.
func (d *DB) Update(ctx context.Context, collection string, id string, update map[string]any) error {
	return stacktrace.Propagate(d.persistStateChange(ctx, StateChange{
		Collection: collection,
		Updates: map[string]map[string]any{
			id: update,
		},
		Timestamp: time.Now(),
	}), "")
}

// BatchUpdate patches a 1-many documents. The documents primary key must be set.
func (d *DB) BatchUpdate(ctx context.Context, collection string, batch map[string]map[string]any) error {
	return stacktrace.Propagate(d.persistStateChange(ctx, StateChange{
		Collection: collection,
		Updates:    batch,
		Timestamp:  time.Now(),
	}), "")
}

// Delete deletes a single document by id
func (d *DB) Delete(ctx context.Context, collection string, id string) error {
	return stacktrace.Propagate(d.persistStateChange(ctx, StateChange{
		Collection: collection,
		Deletes:    []string{id},
		Timestamp:  time.Now(),
	}), "")
}

// BatchDelete deletes 1-many documents by id
func (d *DB) BatchDelete(ctx context.Context, collection string, ids []string) error {
	return stacktrace.Propagate(d.persistStateChange(ctx, StateChange{
		Collection: collection,
		Deletes:    ids,
		Timestamp:  time.Now(),
	}), "")
}

// QueryUpdate updates the documents returned from the query
func (d *DB) QueryUpdate(ctx context.Context, update map[string]any, query Query) error {
	results, err := d.Query(ctx, query)
	if err != nil {
		return stacktrace.Propagate(err, "")
	}
	for _, document := range results.Documents {
		err := document.SetAll(update)
		if err != nil {
			return stacktrace.Propagate(err, "")
		}
	}
	return stacktrace.Propagate(d.BatchSet(ctx, query.From, results.Documents), "")
}

// QueryDelete deletes the documents returned from the query
func (d *DB) QueryDelete(ctx context.Context, query Query) error {
	collect, ok := d.core.GetCollection(ctx, query.From)
	if !ok {
		return stacktrace.NewError("unsupported collection: %s", query.From)
	}
	results, err := d.Query(ctx, query)
	if err != nil {
		return stacktrace.Propagate(err, "")
	}
	var ids []string
	for _, document := range results.Documents {
		ids = append(ids, collect.GetPrimaryKey(document))
	}
	return stacktrace.Propagate(d.BatchDelete(ctx, query.From, ids), "")
}

// Aggregate performs aggregations against the collection
func (d *DB) Aggregate(ctx context.Context, query AggregateQuery) (Page, error) {
	if err := query.Validate(); err != nil {
		return Page{}, nil
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	now := time.Now()
	var results Documents
	match, err := d.core.Scan(ctx, query.From, Scan{
		Filter:  query.Where,
		Reverse: query.OrderBy.Direction == DESC,
	}, func(d *Document) (bool, error) {
		results = append(results, d)
		return true, nil
	})
	if err != nil {
		return Page{}, stacktrace.Propagate(err, "")
	}
	var reduced Documents
	for _, values := range results.GroupBy(query.GroupBy) {
		value, err := values.Aggregate(ctx, query.Aggregates)
		if err != nil {
			return Page{}, stacktrace.Propagate(err, "")
		}
		reduced = append(reduced, value)
	}
	reduced = reduced.OrderBy(query.OrderBy)
	if query.Limit > 0 && query.Page > 0 {
		reduced = lo.Slice(reduced, query.Limit*query.Page, (query.Limit*query.Page)+query.Limit)
	}
	if query.Limit > 0 && len(reduced) > query.Limit {
		reduced = reduced[:query.Limit]
	}

	return Page{
		Documents: reduced.Map(func(t *Document, i int) *Document {
			toSelect := query.GroupBy
			for _, a := range query.Aggregates {
				toSelect = append(toSelect, a.Alias)
			}
			err := t.Select(toSelect)
			if err != nil {
				//	return Page{}, stacktrace.Propagate(err, "")
			}
			return t
		}),
		NextPage: query.Page + 1,
		Count:    len(reduced),
		Stats: PageStats{
			ExecutionTime: time.Since(now),
			IndexMatch:    match,
		},
	}, nil
}

// Query queries a list of documents
func (d *DB) Query(ctx context.Context, query Query) (Page, error) {
	if err := query.Validate(); err != nil {
		return Page{}, nil
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	now := time.Now()
	var results Documents
	fullScan := true
	match, err := d.core.Scan(ctx, query.From, Scan{
		Filter:  query.Where,
		Reverse: query.OrderBy.Direction == DESC,
	}, func(d *Document) (bool, error) {
		results = append(results, d)
		if query.Page == 0 && query.OrderBy.Field == "" && query.Limit > 0 && len(results) >= query.Limit {
			fullScan = false
			return false, nil
		}
		return true, nil
	})
	if err != nil {
		return Page{}, stacktrace.Propagate(err, "")
	}
	results = results.OrderBy(query.OrderBy)

	if fullScan && query.Limit > 0 && query.Page > 0 {
		results = lo.Slice(results, query.Limit*query.Page, (query.Limit*query.Page)+query.Limit)
	}
	if query.Limit > 0 && len(results) > query.Limit {
		results = results[:query.Limit]
	}

	if len(query.Select) > 0 && query.Select[0] != "*" {
		for _, result := range results {
			err := result.Select(query.Select)
			if err != nil {
				return Page{}, stacktrace.Propagate(err, "")
			}
		}
	}
	return Page{
		Documents: results,
		NextPage:  query.Page + 1,
		Count:     len(results),
		Stats: PageStats{
			ExecutionTime: time.Since(now),
			IndexMatch:    match,
		},
	}, nil
}
