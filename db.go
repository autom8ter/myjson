package gokvkit

import (
	"context"
	_ "embed"
	"github.com/autom8ter/gokvkit/internal/safe"
	"github.com/autom8ter/gokvkit/kv"
	"github.com/autom8ter/gokvkit/kv/registry"
	"github.com/autom8ter/machine/v4"
	"github.com/palantir/stacktrace"
	"github.com/samber/lo"
	"github.com/segmentio/ksuid"
	"sync"
	"time"
)

// DB is an embedded, durable NoSQL database with support for schemas, indexing, and aggregation
type DB struct {
	sync.RWMutex
	config      KVConfig
	kv          kv.DB
	machine     machine.Machine
	collections *safe.Map[CollectionConfig]
	optimizer   Optimizer
	validators  *safe.Map[[]ValidatorHook]
	sideEffects *safe.Map[[]SideEffectHook]
	whereHooks  *safe.Map[[]WhereHook]
	readHooks   *safe.Map[[]ReadHook]
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
func New(ctx context.Context, cfg KVConfig, opts ...DBOpt) (*DB, error) {
	db, err := OpenKV(cfg)
	if err != nil {
		return nil, stacktrace.PropagateWithCode(err, ErrTODO, "failed to open kv database")
	}
	d := &DB{
		config:      cfg,
		kv:          db,
		machine:     machine.New(),
		collections: safe.NewMap(map[string]CollectionConfig{}),
		optimizer:   defaultOptimizer{},
		validators:  safe.NewMap(map[string][]ValidatorHook{}),
		sideEffects: safe.NewMap(map[string][]SideEffectHook{}),
		whereHooks:  safe.NewMap(map[string][]WhereHook{}),
		readHooks:   safe.NewMap(map[string][]ReadHook{}),
	}
	coll, err := d.getPersistedCollections()
	if err != nil {
		return nil, stacktrace.PropagateWithCode(err, ErrTODO, "failed to get existing collections")
	}
	d.collections = coll
	for _, o := range opts {
		o(d)
	}
	return d, nil
}

// Get gets a single document by id
func (d *DB) Get(ctx context.Context, collection, id string) (*Document, error) {
	var (
		document *Document
		err      error
	)
	primaryIndex := d.primaryIndex(collection)
	if err := d.kv.Tx(false, func(txn kv.Tx) error {
		val, err := txn.Get(primaryIndex.Seek(map[string]any{
			d.primaryKey(collection): id,
		}).SetDocumentID(id).Path())
		if err != nil {
			return stacktrace.Propagate(err, "")
		}
		document, err = NewDocumentFromBytes(val)
		if err != nil {
			return stacktrace.Propagate(err, "")
		}
		return nil
	}); err != nil {
		return document, stacktrace.Propagate(err, "")
	}
	document, err = d.applyReadHooks(ctx, collection, document)
	if err != nil {
		return document, stacktrace.Propagate(err, "")
	}
	return document, nil
}

// Get gets 1-many document by id(s)
func (d *DB) BatchGet(ctx context.Context, collection string, ids []string) (Documents, error) {
	var documents []*Document
	primaryIndex := d.primaryIndex(collection)
	if err := d.kv.Tx(false, func(txn kv.Tx) error {
		for _, id := range ids {
			value, err := txn.Get(primaryIndex.Seek(map[string]any{
				d.primaryKey(collection): id,
			}).SetDocumentID(id).Path())
			if err != nil {
				return stacktrace.Propagate(err, "")
			}

			document, err := NewDocumentFromBytes(value)
			if err != nil {
				return stacktrace.Propagate(err, "")
			}
			documents = append(documents, document)
		}
		return nil
	}); err != nil {
		return documents, err
	}
	return documents, nil
}

// Create creates a new document - if the documents primary key is unset, it will be set as a sortable unique id
func (d *DB) Create(ctx context.Context, collection string, document *Document) (string, error) {
	if !d.hasCollection(collection) {
		return "", stacktrace.NewError("unsupported collection: %s", collection)
	}
	if d.getPrimaryKey(collection, document) == "" {
		id := ksuid.New().String()
		err := d.setPrimaryKey(collection, document, id)
		if err != nil {
			return "", stacktrace.Propagate(err, "")
		}
	}
	md, _ := GetMetadata(ctx)
	return d.getPrimaryKey(collection, document), stacktrace.Propagate(d.persistStateChange(ctx, collection, StateChange{
		Metadata:   md,
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

	if !d.hasCollection(collection) {
		return nil, stacktrace.NewError("unsupported collection: %s", collection)
	}
	var ids []string
	for _, document := range documents {
		if d.getPrimaryKey(collection, document) == "" {
			id := ksuid.New().String()
			err := d.setPrimaryKey(collection, document, id)
			if err != nil {
				return nil, stacktrace.Propagate(err, "")
			}
		}
		ids = append(ids, d.getPrimaryKey(collection, document))
	}
	md, _ := GetMetadata(ctx)
	if err := d.persistStateChange(ctx, collection, StateChange{
		Metadata:   md,
		Collection: collection,
		Deletes:    nil,
		Creates:    documents,
		Sets:       nil,
		Updates:    nil,
		Timestamp:  time.Now(),
	}); err != nil {
		return nil, stacktrace.Propagate(err, "")
	}
	return ids, nil
}

// Set overwrites a single document. The documents primary key must be set.
func (d *DB) Set(ctx context.Context, collection string, document *Document) error {
	md, _ := GetMetadata(ctx)
	return stacktrace.Propagate(d.persistStateChange(ctx, collection, StateChange{
		Metadata:   md,
		Collection: collection,
		Deletes:    nil,
		Creates:    nil,
		Sets:       []*Document{document},
		Updates:    nil,
		Timestamp:  time.Now(),
	}), "")
}

// BatchSet overwrites 1-many documents. The documents primary key must be set.
func (d *DB) BatchSet(ctx context.Context, collection string, batch []*Document) error {
	md, _ := GetMetadata(ctx)
	return stacktrace.Propagate(d.persistStateChange(ctx, collection, StateChange{
		Metadata:   md,
		Collection: collection,
		Deletes:    nil,
		Creates:    nil,
		Sets:       batch,
		Updates:    nil,
		Timestamp:  time.Now(),
	}), "")
}

// Update patches a single document. The documents primary key must be set.
func (d *DB) Update(ctx context.Context, collection string, id string, update map[string]any) error {
	md, _ := GetMetadata(ctx)
	return stacktrace.Propagate(d.persistStateChange(ctx, collection, StateChange{
		Metadata:   md,
		Collection: collection,
		Updates: map[string]map[string]any{
			id: update,
		},
		Timestamp: time.Now(),
	}), "")
}

// BatchUpdate patches a 1-many documents. The documents primary key must be set.
func (d *DB) BatchUpdate(ctx context.Context, collection string, batch map[string]map[string]any) error {
	md, _ := GetMetadata(ctx)
	return stacktrace.Propagate(d.persistStateChange(ctx, collection, StateChange{
		Metadata:   md,
		Collection: collection,
		Deletes:    nil,
		Creates:    nil,
		Sets:       nil,
		Updates:    batch,
		Timestamp:  time.Now(),
	}), "")
}

// Delete deletes a single document by id
func (d *DB) Delete(ctx context.Context, collection string, id string) error {
	md, _ := GetMetadata(ctx)
	return stacktrace.Propagate(d.persistStateChange(ctx, collection, StateChange{
		Metadata:   md,
		Collection: collection,
		Deletes:    []string{id},
		Creates:    nil,
		Sets:       nil,
		Updates:    nil,
		Timestamp:  time.Now(),
	}), "")
}

// BatchDelete deletes 1-many documents by id
func (d *DB) BatchDelete(ctx context.Context, collection string, ids []string) error {
	md, _ := GetMetadata(ctx)
	return stacktrace.Propagate(d.persistStateChange(ctx, collection, StateChange{
		Metadata:   md,
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

	if !d.hasCollection(query.From) {
		return stacktrace.NewError("unsupported collection: %s", query.From)
	}
	results, err := d.Query(ctx, query)
	if err != nil {
		return stacktrace.Propagate(err, "")
	}
	var ids []string
	for _, document := range results.Documents {
		ids = append(ids, d.getPrimaryKey(query.From, document))
	}
	return stacktrace.Propagate(d.BatchDelete(ctx, query.From, ids), "")
}

// aggregate performs aggregations against the collection
func (d *DB) aggregate(ctx context.Context, query Query) (Page, error) {
	if !d.hasCollection(query.From) {
		return Page{}, stacktrace.NewError("unsupported collection: %s", query.From)
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	now := time.Now()
	var results Documents
	match, err := d.queryScan(ctx, Scan{
		From:  query.From,
		Where: query.Where,
	}, func(d *Document) (bool, error) {
		results = append(results, d)
		return true, nil
	})
	if err != nil {
		return Page{}, stacktrace.Propagate(err, "")
	}
	var reduced Documents
	for _, values := range results.GroupBy(query.GroupBy) {
		value, err := values.Aggregate(ctx, query.Select)
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
			t.Select(query.Select)
			return t
		}),
		NextPage: query.Page + 1,
		Count:    len(reduced),
		Stats: PageStats{
			ExecutionTime:   time.Since(now),
			OptimizerResult: match,
		},
	}, nil
}

// Query queries a list of documents
func (d *DB) Query(ctx context.Context, query Query) (Page, error) {
	if err := query.Validate(); err != nil {
		return Page{}, stacktrace.Propagate(err, "")
	}
	if query.isAggregate() {
		return d.aggregate(ctx, query)
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	now := time.Now()

	if !d.hasCollection(query.From) {
		return Page{}, stacktrace.NewError("unsupported collection: %s", query.From)
	}
	var results Documents
	fullScan := true
	match, err := d.queryScan(ctx, Scan{
		From:  query.From,
		Where: query.Where,
	}, func(d *Document) (bool, error) {
		results = append(results, d)
		if query.Page == 0 && len(query.OrderBy) == 0 && query.Limit > 0 && len(results) >= query.Limit {
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

	if len(query.Select) > 0 && query.Select[0].Field != "*" {
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
			ExecutionTime:   time.Since(now),
			OptimizerResult: match,
		},
	}, nil
}

// Scan scans the optimal index for a collection's documents passing its filters.
// results will not be ordered unless an index supporting the order by(s) was found by the optimizer
// Query should be used when order is more important than performance/resource-usage
func (d *DB) Scan(ctx context.Context, scan Scan, handlerFunc ScanFunc) (OptimizerResult, error) {
	if !d.hasCollection(scan.From) {
		return OptimizerResult{}, stacktrace.NewError("unsupported collection: %s", scan.From)
	}
	return d.queryScan(ctx, scan, handlerFunc)
}

// Close closes the database
func (d *DB) Close(ctx context.Context) error {
	return stacktrace.Propagate(d.kv.Close(), "")
}
