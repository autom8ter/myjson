package myjson

import (
	"context"
	"fmt"

	// import embed package
	_ "embed"
	"sync"
	"time"

	"github.com/autom8ter/machine/v4"
	"github.com/autom8ter/myjson/errors"
	"github.com/autom8ter/myjson/kv"
	"github.com/autom8ter/myjson/kv/registry"
	"github.com/dop251/goja"
	"github.com/samber/lo"
)

// defaultDB is an embedded, durable NoSQL database with support for schemas, indexing, and aggregation
type defaultDB struct {
	ctx           context.Context
	cancel        context.CancelFunc
	wg            sync.WaitGroup
	kv            kv.DB
	machine       machine.Machine
	optimizer     Optimizer
	jsOverrides   map[string]any
	vmPool        chan *goja.Runtime
	collections   sync.Map
	collectionDag *collectionDag
	globalScripts string
}

// Open opens a new database instance from the given config
func Open(ctx context.Context, provider string, providerParams map[string]any, opts ...DBOpt) (Database, error) {
	db, err := registry.Open(provider, providerParams)
	if err != nil {
		return nil, errors.Wrap(err, errors.Internal, "failed to open kv database")
	}
	ctx, cancel := context.WithCancel(ctx)
	d := &defaultDB{
		ctx:           ctx,
		cancel:        cancel,
		wg:            sync.WaitGroup{},
		kv:            db,
		machine:       machine.New(),
		optimizer:     defaultOptimizer{},
		jsOverrides:   map[string]any{},
		vmPool:        make(chan *goja.Runtime, 20),
		collections:   sync.Map{},
		collectionDag: newCollectionDag(),
	}

	for _, o := range opts {
		o(d)
	}

	existing, err := d.getPersistedCollections(context.WithValue(ctx, internalKey, true))
	if err != nil {
		return nil, err
	}
	if err := d.collectionDag.SetSchemas(existing); err != nil {
		return nil, err
	}
	if len(existing) == 0 {
		if err := d.Configure(context.WithValue(ctx, internalKey, true), []string{cdcSchema}); err != nil {
			return nil, errors.Wrap(err, errors.Internal, "failed to configure cdc collection")
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
				var existing []CollectionSchema
				var hasErr bool
				for _, c := range d.Collections(context.WithValue(ctx, internalKey, true)) {
					coll, err := d.getPersistedCollection(context.WithValue(ctx, internalKey, true), c)
					if err != nil {
						fmt.Println(err)
						hasErr = true
					}
					if coll != nil {
						existing = append(existing, coll)
					}
				}
				if !hasErr {
					for _, c := range existing {
						d.collections.Store(c.Collection(), c)
					}
					if err := d.collectionDag.SetSchemas(existing); err != nil {
						fmt.Println(err)
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
	if err := vm.Set(string(JavascriptGlobalTx), tx); err != nil {
		return nil, err
	}
	return &transaction{
		db:      d,
		tx:      tx,
		isBatch: opts.IsBatch,
		vm:      vm,
		docs:    map[string]struct{}{},
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

func (d *defaultDB) dropCollection(ctx context.Context, collection CollectionSchema) error {
	unlock, err := d.lockCollection(ctx, collection.Collection())
	if err != nil {
		return err
	}
	defer unlock()
	if err := d.Tx(ctx, kv.TxOpts{IsBatch: true}, func(ctx context.Context, tx Tx) error {
		_, err := tx.ForEach(ctx, collection.Collection(), ForEachOpts{}, func(d *Document) (bool, error) {
			if err := tx.Delete(ctx, collection.Collection(), collection.GetPrimaryKey(d)); err != nil {
				return false, err
			}
			return true, nil
		})
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	if err := d.kv.DropPrefix(ctx, collectionPrefix(ctx, collection.Collection())); err != nil {
		return errors.Wrap(err, errors.Internal, "failed to remove collection %s", collection)
	}
	if err := d.deleteCollectionConfig(ctx, collection.Collection()); err != nil {
		return errors.Wrap(err, errors.Internal, "failed to remove collection %s", collection)
	}
	return nil
}

func (d *defaultDB) Configure(ctx context.Context, config []string) error {
	var newSchemas []CollectionSchema
	for _, c := range config {
		schema, err := newCollectionSchema([]byte(c))
		if err != nil {
			return err
		}
		newSchemas = append(newSchemas, schema)
	}
	var dag = newCollectionDag()
	if err := dag.SetSchemas(newSchemas); err != nil {
		return errors.Wrap(err, errors.Validation, "failed to set configure schemas")
	}
	sorted, err := d.collectionDag.TopologicalSort()
	if err != nil {
		return errors.Wrap(err, errors.Internal, "failed to topological collections")
	}
	removed, err := d.removedCollections(ctx, sorted, newSchemas)
	if err != nil {
		return errors.Wrap(err, errors.Internal, "failed to get removed collections")
	}

	for _, schema := range sorted {
		exists := false
		for _, remove := range removed {
			if remove.Collection() == schema.Collection() {
				exists = true
				break
			}
		}
		if !exists {
			continue
		}
		pass, err := d.authorizeConfigure(ctx, schema)
		if err != nil {
			return err
		}
		if !pass {
			return errors.New(errors.Forbidden, "not authorized: %s", ConfigureAction)
		}
		if err := d.dropCollection(ctx, schema); err != nil {
			return err
		}
	}

	sorted, err = dag.ReverseTopologicalSort()
	if err != nil {
		return errors.Wrap(err, errors.Internal, "failed to reverse topological collections")
	}
	for _, schema := range sorted {
		before := d.GetSchema(ctx, schema.Collection())
		if before != nil {
			nowBits, _ := schema.MarshalYAML()
			if bits, _ := before.MarshalYAML(); string(bits) == string(nowBits) {
				continue
			}
			pass, err := d.authorizeConfigure(ctx, before)
			if err != nil {
				return err
			}
			if !pass {
				return errors.New(errors.Forbidden, "not authorized: %s", ConfigureAction)
			}
		}
		if err := d.configureCollection(ctx, schema); err != nil {
			return err
		}
	}
	return d.collectionDag.SetSchemas(sorted)
}

func (d *defaultDB) configureCollection(ctx context.Context, collection CollectionSchema) error {
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

func (d *defaultDB) removedCollections(ctx context.Context, existing, config []CollectionSchema) ([]CollectionSchema, error) {
	var removed []CollectionSchema
	var names []string
	for _, c := range config {
		names = append(names, c.Collection())
	}
	for _, schema := range existing {
		if schema.Collection() == cdcCollectionName {
			continue
		}
		if !lo.Contains(names, schema.Collection()) {
			removed = append(removed, schema)
		}
	}
	return removed, nil
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

func (d *defaultDB) ChangeStream(ctx context.Context, collection string, filter []Where, fn ChangeStreamHandler) error {
	if collection != "*" && !d.HasCollection(ctx, collection) {
		return errors.New(errors.Validation, "collection does not exist: %s", collection)
	}
	if !(collection == "*" && isInternal(ctx)) {
		if !d.HasCollection(ctx, collection) {
			return errors.New(errors.Validation, "collection does not exist: %s", collection)
		}
		pass, err := d.authorizeChangeStream(ctx, d.GetSchema(ctx, collection), filter)
		if err != nil {
			return err
		}
		if !pass {
			return errors.New(errors.Forbidden, "not authorized: %s", ChangeStreamAction)
		}
	}

	pfx := indexPrefix(ctx, "system_cdc", "_id.primaryidx")
	return d.kv.ChangeStream(ctx, pfx, func(cdc kv.CDC) (bool, error) {
		doc, _ := NewDocumentFromBytes(cdc.Value)
		pass, err := doc.Where(filter)
		if err != nil {
			return false, err
		}
		if pass {
			var c CDC
			if err := doc.Scan(&c); err != nil {
				return false, errors.Wrap(err, errors.Internal, "failed to unmarshal cdc")
			}
			if c.Collection == collection || collection == "*" {
				return fn(ctx, c)
			}
		}
		return true, nil
	})
}

func (d *defaultDB) RawKV() kv.DB {
	return d.kv
}

func (d *defaultDB) RunScript(ctx context.Context, script string, params map[string]any) (any, error) {
	vm, err := getJavascriptVM(ctx, d, d.jsOverrides)
	if err != nil {
		return false, err
	}
	if err := vm.Set("params", params); err != nil {
		return nil, errors.Wrap(err, errors.Internal, "failed to set params")
	}
	script = d.globalScripts + script
	val, err := vm.RunString(script)
	if err != nil {
		return nil, err
	}
	return val.Export(), nil
}

// NewDoc creates a new document builder
func (d *defaultDB) NewDoc() *DocBuilder {
	return D()
}

func (d *defaultDB) Close(ctx context.Context) error {
	d.cancel()
	<-d.vmPool
	d.machine.Close()
	d.wg.Wait()
	return errors.Wrap(d.kv.Close(ctx), 0, "")
}
