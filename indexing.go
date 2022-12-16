package gokvkit

import (
	"context"
	"fmt"

	"github.com/autom8ter/gokvkit/errors"
	"github.com/autom8ter/gokvkit/internal/safe"
	"github.com/autom8ter/gokvkit/kv"
	"github.com/autom8ter/gokvkit/model"
)

func (d *DB) addIndex(ctx context.Context, collection string, index model.Index) error {
	if index.Name == "" {
		return errors.New(errors.Validation, "%s - empty index name", collection)
	}
	if index.Collection == "" {
		index.Collection = collection
	}
	index.IsBuilding = true
	d.collections.SetFunc(collection, func(c *collectionSchema) *collectionSchema {
		c.indexing[index.Name] = index
		return c
	})
	meta, _ := model.GetMetadata(ctx)
	meta.Set(string(internalKey), true)
	meta.Set(string(isIndexingKey), true)
	if !index.Primary {
		if err := d.Tx(ctx, true, func(ctx context.Context, tx Tx) error {
			_, err := d.Scan(meta.ToContext(ctx), model.Scan{
				From:  collection,
				Where: nil,
			}, func(doc *model.Document) (bool, error) {
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
	d.collections.SetFunc(collection, func(c *collectionSchema) *collectionSchema {
		c.indexing[index.Name] = index
		return c
	})
	return nil
}

func (d *DB) removeIndex(ctx context.Context, collection string, index model.Index) error {
	d.collections.SetFunc(collection, func(c *collectionSchema) *collectionSchema {
		delete(c.indexing, index.Name)
		return c
	})
	meta, _ := model.GetMetadata(ctx)
	meta.Set(string(internalKey), true)
	meta.Set(string(isIndexingKey), true)

	if err := d.Tx(ctx, true, func(ctx context.Context, tx Tx) error {
		_, err := tx.Scan(ctx, model.Scan{
			From: collection,
		}, func(doc *model.Document) (bool, error) {
			if err := tx.Delete(meta.ToContext(ctx), collection, doc.GetString(d.PrimaryKey(collection))); err != nil {
				return false, err
			}
			return true, nil
		})
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return errors.Wrap(err, 0, "indexing: failed to remove index %s - %s", collection, index.Name)
	}
	return nil
}

func (d *DB) persistCollectionConfig(val *collectionSchema) error {
	if val.raw.Raw == "" {
		return errors.New(errors.Validation, "empty collection content")
	}
	if err := d.kv.Tx(true, func(tx kv.Tx) error {
		err := tx.Set([]byte(fmt.Sprintf("internal.collections.%s", val.collection)), val.yamlRaw)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	d.collections.Set(val.collection, val)
	return nil
}

func (d *DB) getPersistedCollections() (*safe.Map[*collectionSchema], error) {
	var (
		collections = safe.NewMap(map[string]*collectionSchema{})
	)
	if err := d.kv.Tx(false, func(tx kv.Tx) error {
		i := tx.NewIterator(kv.IterOpts{
			Prefix: []byte("internal.collections."),
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
				if cfg.yamlRaw == nil {
					return errors.New(errors.Validation, "empty collection yaml content")
				}
				collections.Set(cfg.collection, cfg)
			}
			i.Next()
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return collections, nil
}

func (d *DB) getPersistedCollection(collection string) (*collectionSchema, error) {
	var cfg *collectionSchema
	if err := d.kv.Tx(false, func(tx kv.Tx) error {
		bits, err := tx.Get([]byte(fmt.Sprintf("internal.collections.%s", collection)))
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
