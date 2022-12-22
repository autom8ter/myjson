package gokvkit

import (
	"context"

	"github.com/autom8ter/gokvkit/errors"
	"github.com/autom8ter/gokvkit/kv"
)

func (d *defaultDB) addIndex(ctx context.Context, collection string, index Index) error {
	if index.Name == "" {
		return errors.New(errors.Validation, "%s - empty index name", collection)
	}
	schema := d.getSchema(ctx, collection)
	if schema.Indexing()[index.Name].IsBuilding {
		return errors.New(errors.Forbidden, "%s - index is already building", collection)
	}
	index.IsBuilding = true
	if err := schema.SetIndex(index); err != nil {
		return err
	}
	var err error
	if err := d.persistCollectionConfig(schema); err != nil {
		return err
	}
	if err != nil {
		return err
	}
	meta, _ := GetMetadata(ctx)
	meta.Set(string(internalKey), true)
	meta.Set(string(isIndexingKey), true)
	if !index.Primary {
		if err := d.Tx(ctx, true, func(ctx context.Context, tx Tx) error {
			_, err := d.ForEach(meta.ToContext(ctx), collection, nil, func(doc *Document) (bool, error) {
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
	index.IsBuilding = false
	if err := schema.SetIndex(index); err != nil {
		return err
	}
	if err := d.persistCollectionConfig(schema); err != nil {
		return err
	}
	return err
}

func (d *defaultDB) getSchema(ctx context.Context, collection string) CollectionSchema {
	schema := schemaFromCtx(ctx)
	if schema == nil {
		c, _ := d.getPersistedCollection(collection)
		return c
	}
	return schema
}

func (d *defaultDB) removeIndex(ctx context.Context, collection string, index Index) error {
	var err error
	schema := d.getSchema(ctx, collection)
	if schema.Indexing()[index.Name].IsBuilding {
		return errors.New(errors.Forbidden, "%s - index is already building", collection)
	}
	if err != nil {
		return err
	}
	meta, _ := GetMetadata(ctx)
	meta.Set(string(internalKey), true)
	meta.Set(string(isIndexingKey), true)
	if err := d.kv.DropPrefix(indexPrefix(schema.Collection(), index.Name)); err != nil {
		return errors.Wrap(err, 0, "indexing: failed to remove index %s - %s", collection, index.Name)
	}
	return nil
}

func (d *defaultDB) persistCollectionConfig(val CollectionSchema) error {
	if err := d.kv.Tx(true, func(tx kv.Tx) error {
		bits, err := val.MarshalJSON()
		if err != nil {
			return err
		}
		err = tx.Set(collectionConfigKey(val.Collection()), bits)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func (d *defaultDB) deleteCollectionConfig(collection string) error {
	if err := d.kv.Tx(true, func(tx kv.Tx) error {
		err := tx.Delete(collectionConfigKey(collection))
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func (d *defaultDB) getCollectionConfigs() ([]CollectionSchema, error) {
	var existing []CollectionSchema
	if err := d.kv.Tx(false, func(tx kv.Tx) error {
		i := tx.NewIterator(kv.IterOpts{
			Prefix: collectionConfigPrefix(),
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

func (d *defaultDB) getPersistedCollection(collection string) (CollectionSchema, error) {
	var cfg CollectionSchema
	if err := d.kv.Tx(false, func(tx kv.Tx) error {
		bits, err := tx.Get(collectionConfigKey(collection))
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
