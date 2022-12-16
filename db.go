package gokvkit

import (
	"context"
	_ "embed"

	"github.com/autom8ter/gokvkit/errors"
	"github.com/autom8ter/gokvkit/internal/safe"
	"github.com/autom8ter/gokvkit/kv"
	"github.com/autom8ter/gokvkit/kv/registry"
	"github.com/autom8ter/gokvkit/model"
	"github.com/autom8ter/machine/v4"
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
		return nil, errors.Wrap(err, errors.Internal, "failed to open kv database")
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
		return nil, errors.Wrap(err, errors.Internal, "failed to get existing collections")
	}
	d.collections = coll
	for _, o := range opts {
		o(d)
	}

	d.initHooks.RangeR(func(key string, h OnInit) bool {
		if err = h.Func(ctx, d); err != nil {
			return false
		}
		return true
	})

	return d, err
}

// NewTx returns a new transaction. a transaction must call Commit method in order to persist changes
func (d *DB) NewTx(isUpdate bool) Txn {
	return &transaction{
		db:      d,
		tx:      d.kv.NewTx(isUpdate),
		isBatch: false,
	}
}

// Tx executes the given function against a new transaction.
// if the function returns an error, all changes will be rolled back.
// otherwise, the changes will be commited to the database
func (d *DB) Tx(ctx context.Context, isUpdate bool, fn TxFunc) error {
	tx := d.NewTx(isUpdate)
	defer tx.Close(ctx)
	err := fn(ctx, tx)
	if err != nil {
		tx.Rollback(ctx)
		return errors.Wrap(err, 0, "tx: rolled back transaction")
	}
	if err := tx.Commit(ctx); err != nil {
		return errors.Wrap(err, 0, "tx: failed to commit transaction")
	}
	return nil
}

// Get gets a single document by id
func (d *DB) Get(ctx context.Context, collection, id string) (*model.Document, error) {
	var (
		document *model.Document
		err      error
	)
	if err := d.Tx(ctx, false, func(ctx context.Context, tx Tx) error {
		document, err = tx.Get(ctx, collection, id)
		return err
	}); err != nil {
		return nil, err
	}
	return document, err
}

// Get gets 1-many document by id(s)
func (d *DB) BatchGet(ctx context.Context, collection string, ids []string) (model.Documents, error) {
	var documents []*model.Document
	if err := d.Tx(ctx, false, func(ctx context.Context, tx Tx) error {
		for _, id := range ids {
			document, err := tx.Get(ctx, collection, id)
			if err != nil {
				return err
			}
			documents = append(documents, document)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return documents, nil
}

// Query queries a list of documents
func (d *DB) Query(ctx context.Context, collection string, query model.Query) (model.Page, error) {
	var (
		page model.Page
		err  error
	)
	if err := d.Tx(ctx, false, func(ctx context.Context, tx Tx) error {
		page, err = tx.Query(ctx, collection, query)
		return err
	}); err != nil {
		return model.Page{}, err
	}
	return page, nil
}

// Scan scans the optimal index for a collection's documents passing its filters.
// results will not be ordered unless an index supporting the order by(s) was found by the optimizer
// Query should be used when order is more important than performance/resource-usage
func (d *DB) Scan(ctx context.Context, scan model.Scan, handlerFunc model.ScanFunc) (model.OptimizerResult, error) {
	var (
		result model.OptimizerResult
		err    error
	)
	if err := d.Tx(ctx, false, func(ctx context.Context, tx Tx) error {
		result, err = tx.Scan(ctx, scan, handlerFunc)
		return err
	}); err != nil {
		return result, err
	}
	return result, nil
}

// Close closes the database
func (d *DB) Close(ctx context.Context) error {
	return errors.Wrap(d.kv.Close(), 0, "")
}
