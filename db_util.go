package gokvkit

import (
	"context"
	"fmt"
	"time"

	"github.com/autom8ter/gokvkit/errors"
	"github.com/autom8ter/gokvkit/kv"
)

func (d *defaultDB) lockCollection(ctx context.Context, collection string) (func(), error) {
	md, _ := GetMetadata(ctx)
	lock := d.kv.NewLocker([]byte(fmt.Sprintf("%s.cache.internal.locks.%s", md.GetNamespace(), collection)), 1*time.Minute)
	gotLock, err := lock.TryLock()
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
//			return nil, errors.New(errors.Forbidden, "failed to await lock release on collection: %s", collection)
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

func (d *defaultDB) collectionIsLocked(collection string) bool {
	lock := d.kv.NewLocker([]byte(fmt.Sprintf("cache.internal.locks.%s", collection)), 1*time.Minute)
	is, _ := lock.IsLocked()
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
	meta, _ := GetMetadata(ctx)
	meta.Set(string(internalKey), true)
	meta.Set(string(isIndexingKey), true)
	if !index.Primary {
		if err := d.Tx(ctx, true, func(ctx context.Context, tx Tx) error {
			_, err := d.ForEach(meta.ToContext(ctx), collection, ForEachOpts{}, func(doc *Document) (bool, error) {
				if err := tx.Set(meta.ToContext(ctx), collection, doc); err != nil {
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
		c, _ := d.getPersistedCollection(ctx, collection)
		return c, schemaToCtx(ctx, c)
	}
	return schema, ctx
}

func (d *defaultDB) removeIndex(ctx context.Context, collection string, index Index) error {
	schema, ctx := d.getSchema(ctx, collection)
	if err := d.kv.DropPrefix(indexPrefix(ctx, schema.Collection(), index.Name)); err != nil {
		return errors.Wrap(err, 0, "indexing: failed to remove index %s - %s", collection, index.Name)
	}
	return nil
}

func (d *defaultDB) persistCollectionConfig(ctx context.Context, val CollectionSchema) error {
	if err := d.kv.Tx(true, func(tx kv.Tx) error {
		bits, err := val.MarshalJSON()
		if err != nil {
			return err
		}
		err = tx.Set(collectionConfigKey(ctx, val.Collection()), bits, 0)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func (d *defaultDB) deleteCollectionConfig(ctx context.Context, collection string) error {
	if err := d.kv.Tx(true, func(tx kv.Tx) error {
		err := tx.Delete(collectionConfigKey(ctx, collection))
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func (d *defaultDB) getCollectionConfigs(ctx context.Context) ([]CollectionSchema, error) {
	var existing []CollectionSchema
	if err := d.kv.Tx(false, func(tx kv.Tx) error {
		i := tx.NewIterator(kv.IterOpts{
			Prefix: collectionConfigPrefix(ctx),
		})
		defer i.Close()
		for i.Valid() {
			item := i.Item()

			bits, err := item.Value()
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
			i.Next()
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return existing, nil
}

func (d *defaultDB) getPersistedCollection(ctx context.Context, collection string) (CollectionSchema, error) {
	var cfg CollectionSchema
	if err := d.kv.Tx(false, func(tx kv.Tx) error {
		bits, err := tx.Get(collectionConfigKey(ctx, collection))
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
