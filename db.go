package gokvkit

import (
	"context"
	_ "embed"
	"time"

	"github.com/autom8ter/gokvkit/errors"
	"github.com/autom8ter/gokvkit/kv"
	"github.com/autom8ter/gokvkit/kv/registry"
	"github.com/autom8ter/gokvkit/util"
	"github.com/autom8ter/machine/v4"
)

// defaultDB is an embedded, durable NoSQL database with support for schemas, indexing, and aggregation
type defaultDB struct {
	kv           kv.DB
	machine      machine.Machine
	collections  Cache[CollectionSchema]
	optimizer    Optimizer
	initHooks    Cache[OnInit]
	persistHooks Cache[[]OnPersist]
	onCommit     []OnCommit
	onRollback   []OnRollback
	cdcStream    Stream[CDC]
}

// New creates a new database instance from the given config
func New(ctx context.Context, provider string, providerParams map[string]any, opts ...DBOpt) (Database, error) {
	db, err := registry.Open(provider, providerParams)
	if err != nil {
		return nil, errors.Wrap(err, errors.Internal, "failed to open kv database")
	}
	d := &defaultDB{
		kv:           db,
		machine:      machine.New(),
		collections:  newInMemCache(map[string]CollectionSchema{}),
		optimizer:    defaultOptimizer{},
		initHooks:    newInMemCache(map[string]OnInit{}),
		persistHooks: newInMemCache(map[string][]OnPersist{}),
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

func (d *defaultDB) NewTx(isUpdate bool) Txn {
	return &transaction{
		db:      d,
		tx:      d.kv.NewTx(isUpdate),
		isBatch: false,
	}
}

func (d *defaultDB) Tx(ctx context.Context, isUpdate bool, fn TxFunc) error {
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

func (d *defaultDB) Get(ctx context.Context, collection, id string) (*Document, error) {
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

func (d *defaultDB) BatchGet(ctx context.Context, collection string, ids []string) (Documents, error) {
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

func (d *defaultDB) Query(ctx context.Context, collection string, query Query) (Page, error) {
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

func (d *defaultDB) ForEach(ctx context.Context, collection string, where []Where, fn ForEachFunc) (Optimization, error) {
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

func (d *defaultDB) DropCollection(ctx context.Context, collection string) error {
	if err := d.kv.DropPrefix(collectionPrefix(collection)); err != nil {
		return errors.Wrap(err, errors.Internal, "failed to remove collection %s", collection)
	}
	if err := d.deleteCollectionConfig(collection); err != nil {
		return errors.Wrap(err, errors.Internal, "failed to remove collection %s", collection)
	}
	return nil
}

func (d *defaultDB) ConfigureCollection(ctx context.Context, collectionSchemaBytes []byte) error {
	jsonBytes, err := util.YAMLToJSON(collectionSchemaBytes)
	if err != nil {
		jsonBytes = collectionSchemaBytes
	}
	meta, _ := GetMetadata(ctx)
	meta.Set(string(isIndexingKey), true)
	meta.Set(string(internalKey), true)
	ctx = meta.ToContext(ctx)
	collection, err := newCollectionSchema(jsonBytes)
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

func (d *defaultDB) Collections() []string {
	var names []string
	d.collections.Range(func(key string, c CollectionSchema) bool {
		names = append(names, c.Collection())
		return true
	})
	return names
}

func (d *defaultDB) HasCollection(collection string) bool {
	return d.collections.Exists(collection)
}

func (d *defaultDB) GetSchema(collection string) CollectionSchema {
	return d.collections.Get(collection)
}

func (d *defaultDB) ChangeStream(ctx context.Context, collection string) (<-chan CDC, error) {
	if collection != "*" && !d.HasCollection(collection) {
		return nil, errors.New(errors.Validation, "collection does not exist: %s", collection)
	}
	return d.cdcStream.Pull(ctx, collection)
}

func (d *defaultDB) Close(ctx context.Context) error {
	return errors.Wrap(d.kv.Close(), 0, "")
}
