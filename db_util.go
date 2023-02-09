package myjson

import (
	"context"
	"fmt"
	"time"

	"github.com/autom8ter/myjson/errors"
	"github.com/autom8ter/myjson/kv"
)

func (d *defaultDB) lockCollection(ctx context.Context, collection string) (func(), error) {
	lock, err := d.kv.NewLocker([]byte(fmt.Sprintf("cache.internal.locks.%s", collection)), 1*time.Minute)
	if err != nil {
		return nil, errors.Wrap(err, errors.Internal, "failed to acquire lock on collection %s", collection)
	}
	gotLock, err := lock.TryLock(ctx)
	if err != nil {
		return nil, errors.Wrap(err, errors.Internal, "failed to acquire lock on collection %s", collection)
	}
	if !gotLock {
		return nil, errors.New(errors.Forbidden, "collection: %s is locked", collection)
	}
	return lock.Unlock, nil
}

//
//func (d *defaultDB) awaitCollectionLock(ctx context.Context, ttl time.Duration, collection string) (func(), error) {
//	ctx, cancel := context.WithTimeout(ctx, ttl)
//	defer cancel()
//	ticker := time.NewTicker(50 * time.Millisecond)
//	lock := d.kv.NewLocker([]byte(fmt.Sprintf("cache.internal.locks.%s", collection)), 1*time.Minute)
//	for {
//		select {
//		case <-ctx.Done():
//			return nil, errors.Open(errors.Forbidden, "failed to await lock release on collection: %s", collection)
//		case <-ticker.C:
//			gotLock, err := lock.TryLock()
//			if err != nil {
//				return nil, errors.Wrap(err, errors.Internal, "failed to acquire lock on collection %s", collection)
//			}
//			if !gotLock {
//				continue
//			}
//			return lock.Unlock, nil
//		}
//	}
//}

func (d *defaultDB) collectionIsLocked(ctx context.Context, collection string) bool {
	lock, _ := d.kv.NewLocker([]byte(fmt.Sprintf("cache.internal.locks.%s", collection)), 1*time.Minute)
	if lock == nil {
		return true
	}
	is, _ := lock.IsLocked(ctx)
	return is
}

func (d *defaultDB) addIndex(ctx context.Context, collection string, index Index) error {
	if index.Name == "" {
		return errors.New(errors.Validation, "%s - empty index name", collection)
	}
	schema, ctx := d.getSchema(ctx, collection)
	if err := d.persistCollectionConfig(ctx, schema); err != nil {
		return err
	}
	ctx = context.WithValue(ctx, isIndexingKey, true)
	ctx = context.WithValue(ctx, internalKey, true)

	if !index.Primary {
		if err := d.Tx(ctx, kv.TxOpts{IsReadOnly: false}, func(ctx context.Context, tx Tx) error {
			_, err := d.ForEach(ctx, collection, ForEachOpts{}, func(doc *Document) (bool, error) {
				if err := tx.Set(ctx, collection, doc); err != nil {
					return false, err
				}
				return true, nil
			})
			if err != nil {
				return err
			}
			return nil
		}); err != nil {
			return errors.Wrap(err, 0, "indexing: failed to add index %s - %s", collection, index.Name)
		}
	}
	return nil
}

func (d *defaultDB) getSchema(ctx context.Context, collection string) (CollectionSchema, context.Context) {
	schema := schemaFromCtx(ctx, collection)
	if schema == nil {
		c, _ := d.collections.Load(collection)
		if c == nil {
			return nil, ctx
		}
		return c.(CollectionSchema), schemaToCtx(ctx, c.(CollectionSchema))
	}
	return schema, ctx
}

func (d *defaultDB) removeIndex(ctx context.Context, collection string, index Index) error {
	schema, ctx := d.getSchema(ctx, collection)
	if err := d.kv.DropPrefix(ctx, indexPrefix(ctx, schema.Collection(), index.Name)); err != nil {
		return errors.Wrap(err, 0, "indexing: failed to remove index %s - %s", collection, index.Name)
	}
	return nil
}

func (d *defaultDB) persistCollectionConfig(ctx context.Context, val CollectionSchema) error {
	if err := d.kv.Tx(kv.TxOpts{}, func(tx kv.Tx) error {
		bits, err := val.MarshalJSON()
		if err != nil {
			return err
		}
		err = tx.Set(ctx, collectionConfigKey(ctx, val.Collection()), bits)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	d.collections.Store(val.Collection(), val)
	return nil
}

func (d *defaultDB) deleteCollectionConfig(ctx context.Context, collection string) error {
	if err := d.kv.Tx(kv.TxOpts{}, func(tx kv.Tx) error {
		err := tx.Delete(ctx, collectionConfigKey(ctx, collection))
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	d.collections.Delete(collection)
	return nil
}

func (d *defaultDB) getPersistedCollections(ctx context.Context) ([]CollectionSchema, error) {
	var existing []CollectionSchema
	if err := d.kv.Tx(kv.TxOpts{IsReadOnly: true}, func(tx kv.Tx) error {
		i, err := tx.NewIterator(kv.IterOpts{
			Prefix: collectionConfigPrefix(ctx),
		})
		if err != nil {
			return err
		}
		defer i.Close()
		for i.Valid() {
			bits, err := i.Value()
			if err != nil {
				return err
			}
			if len(bits) > 0 {
				cfg, err := newCollectionSchema(bits)
				if err != nil {
					return err
				}
				existing = append(existing, cfg)
			}
			if err := i.Next(); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return existing, nil
}

func (d *defaultDB) getPersistedCollection(ctx context.Context, collection string) (CollectionSchema, error) {
	var cfg CollectionSchema
	if err := d.kv.Tx(kv.TxOpts{IsReadOnly: true}, func(tx kv.Tx) error {
		bits, err := tx.Get(ctx, collectionConfigKey(ctx, collection))
		if err != nil {
			return err
		}
		cfg, err = newCollectionSchema(bits)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return cfg, err
	}
	if cfg == nil {
		return nil, errors.New(errors.Validation, "collection not found")
	}
	return cfg, nil
}

func (d *defaultDB) getCachedCollections() []CollectionSchema {
	var existing []CollectionSchema
	d.collections.Range(func(key, value interface{}) bool {
		existing = append(existing, value.(CollectionSchema))
		return true
	})
	return existing
}
