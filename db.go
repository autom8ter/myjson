package gokvkit

import (
	"context"
	_ "embed"
	"encoding/json"
	"sync"
	"time"

	"github.com/autom8ter/gokvkit/errors"
	"github.com/autom8ter/gokvkit/kv"
	"github.com/autom8ter/gokvkit/kv/registry"
	"github.com/autom8ter/gokvkit/util"
	"github.com/autom8ter/machine/v4"
	"github.com/dop251/goja"
)

// defaultDB is an embedded, durable NoSQL database with support for schemas, indexing, and aggregation
type defaultDB struct {
	kv          kv.DB
	machine     machine.Machine
	optimizer   Optimizer
	jsOverrides map[string]any
	vmPool      chan *goja.Runtime
	collections sync.Map
}

// New creates a new database instance from the given config
func New(ctx context.Context, provider string, providerParams map[string]any, opts ...DBOpt) (Database, error) {
	db, err := registry.Open(provider, providerParams)
	if err != nil {
		return nil, errors.Wrap(err, errors.Internal, "failed to open kv database")
	}
	d := &defaultDB{
		kv:        db,
		machine:   machine.New(),
		optimizer: defaultOptimizer{},
		vmPool:    make(chan *goja.Runtime, 20),
	}

	for _, o := range opts {
		o(d)
	}
	if !d.HasCollection(ctx, "cdc") {
		if err := d.ConfigureCollection(ctx, []byte(cdcSchema)); err != nil {
			return nil, errors.Wrap(err, errors.Internal, "failed to configure cdc collection")
		}
	}
	if !d.HasCollection(ctx, "migration") {
		if err := d.ConfigureCollection(ctx, []byte(migrationSchema)); err != nil {
			return nil, errors.Wrap(err, errors.Internal, "failed to configure migration collection")
		}
	}
	for _, c := range d.Collections(ctx) {
		coll, err := d.getPersistedCollection(ctx, c)
		if err != nil {
			return nil, err
		}
		if coll != nil {
			d.collections.Store(c, coll)
		}
	}
	go func() {
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				for _, c := range d.Collections(ctx) {
					coll, _ := d.getPersistedCollection(ctx, c)
					if coll != nil {
						d.collections.Store(c, coll)
					}
				}
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

func (d *defaultDB) NewTx(opts kv.TxOpts) (Txn, error) {
	vm := <-d.vmPool
	tx, err := d.kv.NewTx(opts)
	if err != nil {
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

func (d *defaultDB) ForEach(ctx context.Context, collection string, opts ForEachOpts, fn ForEachFunc) (Optimization, error) {
	var (
		result Optimization
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
	if err := d.kv.DropPrefix(ctx, collectionPrefix(ctx, collection)); err != nil {
		return errors.Wrap(err, errors.Internal, "failed to remove collection %s", collection)
	}
	if err := d.deleteCollectionConfig(ctx, collection); err != nil {
		return errors.Wrap(err, errors.Internal, "failed to remove collection %s", collection)
	}
	return nil
}

func (d *defaultDB) ConfigureCollection(ctx context.Context, collectionSchemaBytes []byte) error {
	jsonBytes, err := util.YAMLToJSON(collectionSchemaBytes)
	if err != nil {
		return err
	}
	meta, _ := GetMetadata(ctx)
	meta.Set(string(isIndexingKey), true)
	meta.Set(string(internalKey), true)
	ctx = meta.ToContext(ctx)
	collection, err := newCollectionSchema(jsonBytes)
	if err != nil {
		return err
	}
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
	cfgs, _ := d.getCollectionConfigs(ctx)
	for _, c := range cfgs {
		names = append(names, c.Collection())
	}
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
	pfx := indexPrefix(ctx, "cdc", "_id.primaryidx")
	return d.kv.ChangeStream(ctx, pfx, func(cdc kv.CDC) (bool, error) {
		var c CDC
		if err := json.Unmarshal(cdc.Value, &c); err != nil {
			panic(err)
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

func (d *defaultDB) RunMigrations(ctx context.Context, migrations ...Migration) error {
	var (
		err     error
		skipped bool
	)
	for _, m := range migrations {
		m.Dirty = false
		m.Timestamp = time.Now().Unix()
		m.Error = ""
		skipped, err = d.runMigration(ctx, m)
		if skipped {
			continue
		}
		if err != nil {
			m.Error = err.Error()
			m.Dirty = true
		}
		doc, err := NewDocumentFrom(m)
		if err != nil {
			return err
		}
		if err := d.Tx(ctx, kv.TxOpts{IsReadOnly: false}, func(ctx context.Context, tx Tx) error {
			if err := tx.Set(ctx, migrationCollectionName, doc); err != nil {
				return err
			}
			return nil
		}); err != nil {
			return err
		}
		if err != nil {
			break
		}
	}
	return err
}

func (d *defaultDB) runMigration(ctx context.Context, m Migration) (bool, error) {
	if val, _ := d.Get(ctx, migrationCollectionName, m.ID); val != nil && !val.GetBool("dirty") {
		return true, nil
	}
	if err := util.ValidateStruct(m); err != nil {
		return false, errors.Wrap(err, 0, "migration is not valid")
	}
	vm, err := getJavascriptVM(ctx, d, d.jsOverrides)
	if err != nil {
		return false, err
	}
	_, err = vm.RunString(m.Script)
	if err != nil {
		return false, err
	}
	return false, nil
}

func (d *defaultDB) Close(ctx context.Context) error {
	return errors.Wrap(d.kv.Close(ctx), 0, "")
}
