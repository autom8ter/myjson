package myjson

import (
	"context"
	// import embed package
	_ "embed"
	"encoding/json"
	"sync"
	"time"

	"github.com/autom8ter/machine/v4"
	"github.com/autom8ter/myjson/errors"
	"github.com/autom8ter/myjson/kv"
	"github.com/autom8ter/myjson/kv/registry"
	"github.com/autom8ter/myjson/util"
	"github.com/dop251/goja"
)

// defaultDB is an embedded, durable NoSQL database with support for schemas, indexing, and aggregation
type defaultDB struct {
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	kv          kv.DB
	machine     machine.Machine
	optimizer   Optimizer
	jsOverrides map[string]any
	vmPool      chan *goja.Runtime
	collections sync.Map
}

// Open opens a new database instance from the given config
func Open(ctx context.Context, provider string, providerParams map[string]any, opts ...DBOpt) (Database, error) {
	db, err := registry.Open(provider, providerParams)
	if err != nil {
		return nil, errors.Wrap(err, errors.Internal, "failed to open kv database")
	}
	ctx, cancel := context.WithCancel(ctx)
	d := &defaultDB{
		ctx:         ctx,
		cancel:      cancel,
		wg:          sync.WaitGroup{},
		kv:          db,
		machine:     machine.New(),
		optimizer:   defaultOptimizer{},
		jsOverrides: map[string]any{},
		vmPool:      make(chan *goja.Runtime, 20),
		collections: sync.Map{},
	}

	for _, o := range opts {
		o(d)
	}
	if !d.HasCollection(ctx, cdcCollectionName) {
		if err := d.Configure(context.WithValue(ctx, internalKey, true), []byte(cdcSchema)); err != nil {
			return nil, errors.Wrap(err, errors.Internal, "failed to configure cdc collection")
		}
	}
	for _, c := range d.Collections(context.WithValue(ctx, internalKey, true)) {
		coll, err := d.getPersistedCollection(context.WithValue(ctx, internalKey, true), c)
		if err != nil {
			return nil, err
		}
		if coll != nil {
			d.collections.Store(c, coll)
		}
	}
	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				for _, c := range d.Collections(context.WithValue(ctx, internalKey, true)) {
					coll, _ := d.getPersistedCollection(context.WithValue(ctx, internalKey, true), c)
					if coll != nil {
						d.collections.Store(c, coll)
					}
				}
			}
		}
	}()
	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
				vm, _ := getJavascriptVM(ctx, d, d.jsOverrides)
				if vm != nil {
					d.vmPool <- vm
				}
			}
		}
	}()
	return d, err
}

func (d *defaultDB) Serve(ctx context.Context, t Transport) error {
	return t.Serve(ctx, d)
}

func (d *defaultDB) NewTx(opts kv.TxOpts) (Txn, error) {
	vm := <-d.vmPool
	tx, err := d.kv.NewTx(opts)
	if err != nil {
		return nil, err
	}
	if err := vm.Set("tx", tx); err != nil {
		return nil, err
	}
	return &transaction{
		db:      d,
		tx:      tx,
		isBatch: false,
		vm:      vm,
	}, nil
}

func (d *defaultDB) Tx(ctx context.Context, opts kv.TxOpts, fn TxFunc) error {
	tx, err := d.NewTx(opts)
	if err != nil {
		return err
	}
	defer tx.Close(ctx)
	if err := fn(ctx, tx); err != nil {
		if rollbackError := tx.Rollback(ctx); rollbackError != nil {
			return errors.Wrap(err, 0, "failed to rollback transaction: "+rollbackError.Error())
		}
		return errors.Wrap(err, 0, "tx: rolled back transaction")
	}
	if err := tx.Commit(ctx); err != nil {
		if rollbackError := tx.Rollback(ctx); rollbackError != nil {
			return errors.Wrap(err, 0, "failed to rollback transaction: "+rollbackError.Error())
		}
		return errors.Wrap(err, 0, "tx: failed to commit transaction - rolled back")
	}
	return nil
}

func (d *defaultDB) Get(ctx context.Context, collection, id string) (*Document, error) {
	var (
		document *Document
		err      error
	)

	// Tx(ctx, kv.TxOpts{IsReadOnly: true},
	if err := d.Tx(ctx, kv.TxOpts{IsReadOnly: true}, func(ctx context.Context, tx Tx) error {
		document, err = tx.Get(ctx, collection, id)
		return err
	}); err != nil {
		return nil, err
	}
	return document, err
}

func (d *defaultDB) BatchGet(ctx context.Context, collection string, ids []string) (Documents, error) {
	var documents []*Document
	if err := d.Tx(ctx, kv.TxOpts{IsReadOnly: true}, func(ctx context.Context, tx Tx) error {
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
	if len(query.Select) == 0 {
		query.Select = []Select{{Field: "*"}}
	}
	if err := d.Tx(ctx, kv.TxOpts{IsReadOnly: true}, func(ctx context.Context, tx Tx) error {
		page, err = tx.Query(ctx, collection, query)
		return err
	}); err != nil {
		return Page{}, err
	}
	return page, nil
}

func (d *defaultDB) ForEach(ctx context.Context, collection string, opts ForEachOpts, fn ForEachFunc) (Explain, error) {
	var (
		result Explain
		err    error
	)
	if err := d.Tx(ctx, kv.TxOpts{IsReadOnly: true}, func(ctx context.Context, tx Tx) error {
		result, err = tx.ForEach(ctx, collection, opts, fn)
		return err
	}); err != nil {
		return result, err
	}
	return result, nil
}

func (d *defaultDB) DropCollection(ctx context.Context, collection string) error {
	unlock, err := d.lockCollection(ctx, collection)
	if err != nil {
		return err
	}
	defer unlock()
	if err := d.kv.DropPrefix(ctx, collectionPrefix(ctx, collection)); err != nil {
		return errors.Wrap(err, errors.Internal, "failed to remove collection %s", collection)
	}
	if err := d.deleteCollectionConfig(ctx, collection); err != nil {
		return errors.Wrap(err, errors.Internal, "failed to remove collection %s", collection)
	}
	return nil
}

func (d *defaultDB) Configure(ctx context.Context, collectionSchemaBytes []byte) error {
	jsonBytes, err := util.YAMLToJSON(collectionSchemaBytes)
	if err != nil {
		return err
	}
	collection, err := newCollectionSchema(jsonBytes)
	if err != nil {
		return err
	}

	before := d.GetSchema(ctx, collection.Collection())
	if before != nil {
		pass, err := d.authorizeConfigure(ctx, before)
		if err != nil {
			return err
		}
		if !pass {
			return errors.New(errors.Forbidden, "not authorized: %s", ConfigureAction)
		}
	}
	ctx = context.WithValue(ctx, isIndexingKey, true)
	ctx = context.WithValue(ctx, internalKey, true)
	unlock, err := d.lockCollection(ctx, collection.Collection())
	if err != nil {
		return err
	}
	defer unlock()

	if err := d.persistCollectionConfig(ctx, collection); err != nil {
		return err
	}

	existing, _ := d.getPersistedCollection(ctx, collection.Collection())
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
	if err := d.persistCollectionConfig(ctx, collection); err != nil {
		return err
	}
	return nil
}

func (d *defaultDB) Collections(ctx context.Context) []string {
	var names []string
	d.collections.Range(func(key, value any) bool {
		names = append(names, key.(string))
		return true
	})
	return names
}

func (d *defaultDB) HasCollection(ctx context.Context, collection string) bool {
	s, _ := d.getSchema(ctx, collection)
	return s != nil && s.Collection() != ""
}

func (d *defaultDB) GetSchema(ctx context.Context, collection string) CollectionSchema {
	s, _ := d.getSchema(ctx, collection)
	return s
}

func (d *defaultDB) ChangeStream(ctx context.Context, collection string, fn func(cdc CDC) (bool, error)) error {
	if collection != "*" && !d.HasCollection(ctx, collection) {
		return errors.New(errors.Validation, "collection does not exist: %s", collection)
	}
	if !(collection == "*" && isInternal(ctx)) {
		if !d.HasCollection(ctx, collection) {
			return errors.New(errors.Validation, "collection does not exist: %s", collection)
		}
		pass, err := d.authorizeChangeStream(ctx, d.GetSchema(ctx, collection))
		if err != nil {
			return err
		}
		if !pass {
			return errors.New(errors.Forbidden, "not authorized: %s", ChangeStreamAction)
		}
	}

	pfx := indexPrefix(ctx, "cdc", "_id.primaryidx")
	return d.kv.ChangeStream(ctx, pfx, func(cdc kv.CDC) (bool, error) {
		var c CDC
		if err := json.Unmarshal(cdc.Value, &c); err != nil {
			return false, errors.Wrap(err, errors.Internal, "failed to unmarshal cdc")
		}
		if c.Collection == collection || collection == "*" {
			return fn(c)
		}
		return true, nil
	})
}

func (d *defaultDB) RawKV() kv.DB {
	return d.kv
}

func (d *defaultDB) RunScript(ctx context.Context, function string, script string, params map[string]any) (any, error) {
	vm, err := getJavascriptVM(ctx, d, d.jsOverrides)
	if err != nil {
		return false, err
	}
	_, err = vm.RunString(script)
	if err != nil {
		return nil, err
	}
	var fn func(ctx context.Context, db Database, params map[string]any) (any, error)
	if err := vm.ExportTo(vm.Get(function), &fn); err != nil {
		return nil, errors.Wrap(err, errors.Validation, "failed to export")
	}
	return fn(ctx, d, params)
}

func (d *defaultDB) Close(ctx context.Context) error {
	d.cancel()
	<-d.vmPool
	d.machine.Close()
	d.wg.Wait()
	return errors.Wrap(d.kv.Close(ctx), 0, "")
}

// NewDoc creates a new document builder
func (d *defaultDB) NewDoc() *DocBuilder {
	return D()
}
