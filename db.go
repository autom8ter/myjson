package gokvkit

import (
	"context"
	_ "embed"
	"time"

	"github.com/autom8ter/gokvkit/errors"
	"github.com/autom8ter/gokvkit/kv"
	"github.com/autom8ter/gokvkit/kv/registry"
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
	collections  Cache[CollectionSchema]
	optimizer    Optimizer
	initHooks    Cache[OnInit]
	persistHooks Cache[[]OnPersist]
	whereHooks   Cache[[]OnWhere]
	readHooks    Cache[[]OnRead]
	onCommit     []OnCommit
	onRollback   []OnRollback
	cdcStream    Stream[CDC]
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
		collections:  newInMemCache(map[string]CollectionSchema{}),
		optimizer:    defaultOptimizer{},
		initHooks:    newInMemCache(map[string]OnInit{}),
		persistHooks: newInMemCache(map[string][]OnPersist{}),
		whereHooks:   newInMemCache(map[string][]OnWhere{}),
		readHooks:    newInMemCache(map[string][]OnRead{}),
		cdcStream:    newStream[CDC](machine.New()),
	}

	for _, o := range opts {
		o(d)
	}
	if err := d.refreshCollections(); err != nil {
		return nil, errors.Wrap(err, errors.Internal, "failed to get load collections")
	}
	d.initHooks.Range(func(key string, h OnInit) bool {
		if err = h.Func(ctx, d); err != nil {
			return false
		}
		return true
	})
	if err := d.ConfigureCollection(ctx, []byte(cdcSchema)); err != nil {
		return nil, errors.Wrap(err, errors.Internal, "failed to configure cdc collection")
	}
	go func() {
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		ticker := time.NewTicker(1 * time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				d.refreshCollections()
			}
		}
	}()
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
		tx.Rollback(ctx)
		return errors.Wrap(err, 0, "tx: failed to commit transaction - rolled back")
	}
	return nil
}

// Get gets a single document by id
func (d *DB) Get(ctx context.Context, collection, id string) (*Document, error) {
	var (
		document *Document
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
func (d *DB) BatchGet(ctx context.Context, collection string, ids []string) (Documents, error) {
	var documents []*Document
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
func (d *DB) Query(ctx context.Context, collection string, query Query) (Page, error) {
	var (
		page Page
		err  error
	)
	if err := d.Tx(ctx, false, func(ctx context.Context, tx Tx) error {
		page, err = tx.Query(ctx, collection, query)
		return err
	}); err != nil {
		return Page{}, err
	}
	return page, nil
}

// ForEach scans the optimal index for a collection's documents passing its filters.
// results will not be ordered unless an index supporting the order by(s) was found by the optimizer
// Query should be used when order is more important than performance/resource-usage
func (d *DB) ForEach(ctx context.Context, collection string, where []Where, fn ForEachFunc) (Optimization, error) {
	var (
		result Optimization
		err    error
	)
	if err := d.Tx(ctx, false, func(ctx context.Context, tx Tx) error {
		result, err = tx.ForEach(ctx, collection, where, fn)
		return err
	}); err != nil {
		return result, err
	}
	return result, nil
}

// DropCollection drops the collection and it's indexes from the database
func (d *DB) DropCollection(ctx context.Context, collection string) error {
	if err := d.kv.DropPrefix(collectionPrefix(collection)); err != nil {
		return errors.Wrap(err, errors.Internal, "failed to remove collection %s", collection)
	}
	if err := d.deleteCollectionConfig(collection); err != nil {
		return errors.Wrap(err, errors.Internal, "failed to remove collection %s", collection)
	}
	return nil
}

// ConfigureCollection overwrites a single database collection configuration
func (d *DB) ConfigureCollection(ctx context.Context, collectionSchemaBytes []byte) error {
	meta, _ := GetMetadata(ctx)
	meta.Set(string(isIndexingKey), true)
	meta.Set(string(internalKey), true)
	ctx = meta.ToContext(ctx)
	collection, err := newCollectionSchema(collectionSchemaBytes)
	if err != nil {
		return err
	}

	if err := d.persistCollectionConfig(collection); err != nil {
		return err
	}

	existing, _ := d.getPersistedCollection(collection.Collection())
	var diff indexDiff
	if existing == nil {
		diff, err = getIndexDiff(collection.Indexing(), map[string]Index{})
		if err != nil {
			return err
		}
	} else {
		diff, err = getIndexDiff(collection.Indexing(), existing.Indexing())
		if err != nil {
			return err
		}
	}
	for _, update := range diff.toUpdate {
		if err := d.removeIndex(ctx, collection.Collection(), update); err != nil {
			return err
		}
		if err := d.addIndex(ctx, collection.Collection(), update); err != nil {
			return err
		}
	}
	for _, toDelete := range diff.toRemove {
		if err := d.removeIndex(ctx, collection.Collection(), toDelete); err != nil {
			return err
		}
	}
	for _, toAdd := range diff.toAdd {
		if err := d.addIndex(ctx, collection.Collection(), toAdd); err != nil {
			return err
		}
	}
	if err := d.persistCollectionConfig(collection); err != nil {
		return err
	}
	return nil
}

// Collections returns a list of collection names that are registered in the collection
func (d *DB) Collections() []string {
	var names []string
	d.collections.Range(func(key string, c CollectionSchema) bool {
		names = append(names, c.Collection())
		return true
	})
	return names
}

// HasCollection reports whether a collection exists in the database
func (d *DB) HasCollection(collection string) bool {
	return d.collections.Exists(collection)
}

// GetSchema gets a collection schema by name (if it exists)
func (d *DB) GetSchema(collection string) CollectionSchema {
	return d.collections.Get(collection)
}

// ChangeStream streams changes to documents in the given collection.
func (d *DB) ChangeStream(ctx context.Context, collection string) (<-chan CDC, error) {
	if collection != "*" && !d.HasCollection(collection) {
		return nil, errors.New(errors.Validation, "collection does not exist: %s", collection)
	}
	return d.cdcStream.Pull(ctx, collection)
}

// Close closes the database
func (d *DB) Close(ctx context.Context) error {
	return errors.Wrap(d.kv.Close(), 0, "")
}
