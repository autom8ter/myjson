package gokvkit

import (
	"context"
	_ "embed"
	"time"

	"github.com/autom8ter/gokvkit/internal/safe"
	"github.com/autom8ter/gokvkit/internal/util"
	"github.com/autom8ter/gokvkit/kv"
	"github.com/autom8ter/gokvkit/kv/registry"
	"github.com/autom8ter/gokvkit/model"
	"github.com/autom8ter/machine/v4"
	"github.com/palantir/stacktrace"
	"github.com/samber/lo"
)

// KVConfig configures a key value database from the given provider
type KVConfig struct {
	// Provider is the name of the kv provider (badger)
	Provider string `json:"provider"`
	// Params are the kv providers params
	Params map[string]any `json:"params"`
}

// Config configures a database instance
type Config struct {
	// KV is the key value configuration
	KV KVConfig `json:"kv"`
}

// DB is an embedded, durable NoSQL database with support for schemas, indexing, and aggregation
type DB struct {
	config       Config
	kv           kv.DB
	machine      machine.Machine
	collections  *safe.Map[*collectionSchema]
	optimizer    Optimizer
	initHooks    *safe.Map[OnInit]
	persistHooks *safe.Map[[]OnPersist]
	whereHooks   *safe.Map[[]OnWhere]
	readHooks    *safe.Map[[]OnRead]
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
func New(ctx context.Context, cfg Config, opts ...DBOpt) (*DB, error) {
	db, err := OpenKV(cfg.KV)
	if err != nil {
		return nil, stacktrace.PropagateWithCode(err, ErrTODO, "failed to open kv database")
	}
	d := &DB{
		config:       cfg,
		kv:           db,
		machine:      machine.New(),
		collections:  safe.NewMap(map[string]*collectionSchema{}),
		optimizer:    defaultOptimizer{},
		initHooks:    safe.NewMap(map[string]OnInit{}),
		persistHooks: safe.NewMap(map[string][]OnPersist{}),
		whereHooks:   safe.NewMap(map[string][]OnWhere{}),
		readHooks:    safe.NewMap(map[string][]OnRead{}),
	}
	coll, err := d.getPersistedCollections()
	if err != nil {
		return nil, stacktrace.PropagateWithCode(err, ErrTODO, "failed to get existing collections")
	}
	d.collections = coll
	for _, o := range opts {
		o(d)
	}

	d.initHooks.RangeR(func(key string, h OnInit) bool {
		if err = h.Func(ctx, d); err != nil {
			err = stacktrace.Propagate(err, "")
			return false
		}
		return true
	})

	return d, err
}

// NewTx returns a new transaction. a transaction must call Commit method in order to persist changes
func (d *DB) NewTx() Tx {
	return &transaction{db: d}
}

// Tx executs the given function against a new transaction.
// if the function returns an error, all changes will be rolled back.
// otherwise, the changes will be commited to the database
func (d *DB) Tx(ctx context.Context, fn TxFunc) error {
	tx := d.NewTx()
	err := fn(ctx, tx)
	if err != nil {
		tx.Rollback(ctx)
		return stacktrace.Propagate(err, "rolled back transaction")
	}
	return stacktrace.Propagate(tx.Commit(ctx), "failed to commit transaction")
}

// Get gets a single document by id
func (d *DB) Get(ctx context.Context, collection, id string) (*model.Document, error) {
	var (
		document *model.Document
		err      error
	)
	primaryIndex := d.primaryIndex(collection)
	if err := d.kv.Tx(false, func(txn kv.Tx) error {
		val, err := txn.Get(primaryIndex.SeekPrefix(map[string]any{
			d.PrimaryKey(collection): id,
		}).SetDocumentID(id).Path())
		if err != nil {
			return stacktrace.Propagate(err, "")
		}
		document, err = model.NewDocumentFromBytes(val)
		if err != nil {
			return stacktrace.Propagate(err, "")
		}
		return nil
	}); err != nil {
		return nil, stacktrace.Propagate(err, "")
	}
	document, err = d.applyReadHooks(ctx, collection, document)
	if err != nil {
		return document, stacktrace.Propagate(err, "")
	}
	return document, nil
}

// Get gets 1-many document by id(s)
func (d *DB) BatchGet(ctx context.Context, collection string, ids []string) (model.Documents, error) {
	var documents []*model.Document
	primaryIndex := d.primaryIndex(collection)
	if err := d.kv.Tx(false, func(txn kv.Tx) error {
		for _, id := range ids {
			value, err := txn.Get(primaryIndex.SeekPrefix(map[string]any{
				d.PrimaryKey(collection): id,
			}).SetDocumentID(id).Path())
			if err != nil {
				return stacktrace.Propagate(err, "")
			}

			document, err := model.NewDocumentFromBytes(value)
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

// aggregate performs aggregations against the collection
func (d *DB) aggregate(ctx context.Context, collection string, query model.Query) (model.Page, error) {
	if !d.HasCollection(collection) {
		return model.Page{}, stacktrace.NewError("unsupported collection: %s", collection)
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	now := time.Now()
	var results model.Documents
	match, err := d.queryScan(ctx, model.Scan{
		From:  collection,
		Where: query.Where,
	}, func(d *model.Document) (bool, error) {
		results = append(results, d)
		return true, nil
	})
	if err != nil {
		return model.Page{}, stacktrace.Propagate(err, "")
	}
	var reduced model.Documents
	for _, values := range model.GroupByDocs(results, query.GroupBy) {
		value, err := model.AggregateDocs(values, query.Select)
		if err != nil {
			return model.Page{}, stacktrace.Propagate(err, "")
		}
		reduced = append(reduced, value)
	}
	reduced = model.OrderByDocs(reduced, query.OrderBy)
	if (!util.IsNil(query.Limit) && *query.Limit > 0) && (!util.IsNil(query.Limit) && *query.Page > 0) {
		reduced = lo.Slice(reduced, *query.Limit**query.Page, (*query.Limit**query.Page)+*query.Limit)
	}
	if !util.IsNil(query.Limit) && *query.Limit > 0 && len(reduced) > *query.Limit {
		reduced = reduced[:*query.Limit]
	}
	if query.Page == nil {
		query.Page = util.ToPtr(0)
	}
	return model.Page{
		Documents: reduced,
		NextPage:  *query.Page + 1,
		Count:     len(reduced),
		Stats: model.PageStats{
			ExecutionTime:   time.Since(now),
			OptimizerResult: match,
		},
	}, nil
}

// Query queries a list of documents
func (d *DB) Query(ctx context.Context, collection string, query model.Query) (model.Page, error) {
	if err := query.Validate(ctx); err != nil {
		return model.Page{}, stacktrace.Propagate(err, "")
	}
	if query.IsAggregate() {
		return d.aggregate(ctx, collection, query)
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	now := time.Now()

	if !d.HasCollection(collection) {
		return model.Page{}, stacktrace.NewError("unsupported collection: %s", collection)
	}
	var results model.Documents
	fullScan := true
	match, err := d.queryScan(ctx, model.Scan{
		From:  collection,
		Where: query.Where,
	}, func(d *model.Document) (bool, error) {
		results = append(results, d)
		if query.Page != nil && *query.Page == 0 && len(query.OrderBy) == 0 && *query.Limit > 0 && len(results) >= *query.Limit {
			fullScan = false
			return false, nil
		}
		return true, nil
	})
	if err != nil {
		return model.Page{}, stacktrace.Propagate(err, "")
	}
	results = model.OrderByDocs(results, query.OrderBy)

	if fullScan && !util.IsNil(query.Limit) && !util.IsNil(query.Page) && *query.Limit > 0 && *query.Page > 0 {
		results = lo.Slice(results, *query.Limit**query.Page, (*query.Limit**query.Page)+*query.Limit)
	}
	if !util.IsNil(query.Limit) && *query.Limit > 0 && len(results) > *query.Limit {
		results = results[:*query.Limit]
	}

	if len(query.Select) > 0 && query.Select[0].Field != "*" {
		for _, result := range results {
			err := result.Select(query.Select)
			if err != nil {
				return model.Page{}, stacktrace.Propagate(err, "")
			}
		}
	}
	if query.Page == nil {
		query.Page = util.ToPtr(0)
	}
	return model.Page{
		Documents: results,
		NextPage:  *query.Page + 1,
		Count:     len(results),
		Stats: model.PageStats{
			ExecutionTime:   time.Since(now),
			OptimizerResult: match,
		},
	}, nil
}

// Scan scans the optimal index for a collection's documents passing its filters.
// results will not be ordered unless an index supporting the order by(s) was found by the optimizer
// Query should be used when order is more important than performance/resource-usage
func (d *DB) Scan(ctx context.Context, scan model.Scan, handlerFunc model.ScanFunc) (model.OptimizerResult, error) {
	if !d.HasCollection(scan.From) {
		return model.OptimizerResult{}, stacktrace.NewError("unsupported collection: %s", scan.From)
	}
	return d.queryScan(ctx, scan, handlerFunc)
}

// Close closes the database
func (d *DB) Close(ctx context.Context) error {
	return stacktrace.Propagate(d.kv.Close(), "")
}
