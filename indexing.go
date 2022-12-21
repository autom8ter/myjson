package gokvkit

import (
	"bytes"
	"context"
	"fmt"

	"github.com/autom8ter/gokvkit/errors"
	"github.com/autom8ter/gokvkit/kv"
	"github.com/autom8ter/gokvkit/util"
	"github.com/nqd/flat"
)

func (d *DB) addIndex(ctx context.Context, collection string, index Index) error {
	if index.Name == "" {
		return errors.New(errors.Validation, "%s - empty index name", collection)
	}
	if d.collections.Get(collection).Indexing()[index.Name].IsBuilding {
		return errors.New(errors.Forbidden, "%s - index is already building", collection)
	}
	index.IsBuilding = true
	var err error
	d.collections.SetFunc(collection, func(c CollectionSchema) CollectionSchema {
		err = c.SetIndex(index)
		return c
	})
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
	d.collections.SetFunc(collection, func(c CollectionSchema) CollectionSchema {
		err = c.SetIndex(index)
		return c
	})
	return err
}

func (d *DB) removeIndex(ctx context.Context, collection string, index Index) error {
	var err error
	if d.collections.Get(collection).Indexing()[index.Name].IsBuilding {
		return errors.New(errors.Forbidden, "%s - index is already building", collection)
	}
	d.collections.SetFunc(collection, func(c CollectionSchema) CollectionSchema {
		err = c.DelIndex(index.Name)
		return c
	})
	if err != nil {
		return err
	}
	meta, _ := GetMetadata(ctx)
	meta.Set(string(internalKey), true)
	meta.Set(string(isIndexingKey), true)
	if err := d.kv.DropPrefix(indexPrefix(collection, index.Name)); err != nil {
		return errors.Wrap(err, 0, "indexing: failed to remove index %s - %s", collection, index.Name)
	}
	return nil
}

func (d *DB) persistCollectionConfig(val CollectionSchema) error {
	if err := d.kv.Tx(true, func(tx kv.Tx) error {
		bits, err := val.Bytes()
		if err != nil {
			return err
		}
		err = tx.Set([]byte(fmt.Sprintf("internal.collections.%s", val.Collection())), bits)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	d.collections.Set(val.Collection(), val)
	return nil
}

func (d *DB) deleteCollectionConfig(collection string) error {
	if err := d.kv.Tx(true, func(tx kv.Tx) error {
		err := tx.Delete([]byte(fmt.Sprintf("internal.collections.%s", collection)))
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	d.collections.Del(collection)
	return nil
}

func (d *DB) refreshCollections() error {
	var existing map[string]struct{}
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
				existing[cfg.Collection()] = struct{}{}
				d.collections.Set(cfg.Collection(), cfg)
			}
			i.Next()
		}
		return nil
	}); err != nil {
		return err
	}
	var toDelete []string
	d.collections.Range(func(key string, c CollectionSchema) bool {
		if _, ok := existing[key]; !ok {
			toDelete = append(toDelete, key)
		}
		return true
	})
	for _, del := range toDelete {
		d.collections.Del(del)
	}
	return nil
}

func (d *DB) getPersistedCollection(collection string) (CollectionSchema, error) {
	var cfg CollectionSchema
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

// indexFieldValue is a key value pair
type indexFieldValue struct {
	Field string `json:"field"`
	Value any    `json:"value"`
}

func seekPrefix(collection string, i Index, fields map[string]any) indexPathPrefix {
	fields, _ = flat.Flatten(fields, nil)
	var prefix = indexPathPrefix{
		prefix: [][]byte{
			[]byte("index"),
			[]byte(collection),
			[]byte(i.Name),
		},
	}
	if i.Fields == nil {
		return prefix
	}
	for _, k := range i.Fields {
		if v, ok := fields[k]; ok {
			prefix = prefix.Append(k, v)
		}
	}
	return prefix
}

type indexPathPrefix struct {
	prefix     [][]byte
	documentID string
	fields     [][]byte
	fieldMap   []indexFieldValue
}

func (p indexPathPrefix) Append(field string, value any) indexPathPrefix {
	fields := append(p.fields, []byte(field), util.EncodeIndexValue(value))
	fieldMap := append(p.fieldMap, indexFieldValue{
		Field: field,
		Value: value,
	})
	return indexPathPrefix{
		prefix:   p.prefix,
		fields:   fields,
		fieldMap: fieldMap,
	}
}

func (p indexPathPrefix) SetDocumentID(id string) indexPathPrefix {
	return indexPathPrefix{
		prefix:     p.prefix,
		documentID: id,
		fields:     p.fields,
		fieldMap:   p.fieldMap,
	}
}

func (p indexPathPrefix) Path() []byte {
	var path = append(p.prefix, p.fields...)
	if p.documentID != "" {
		path = append(path, []byte(p.documentID))
	}
	return bytes.Join(path, []byte("\x00"))
}

func (i indexPathPrefix) DocumentID() string {
	return i.documentID
}

func (i indexPathPrefix) Fields() []indexFieldValue {
	return i.fieldMap
}

func indexPrefix(collection, index string) []byte {
	path := [][]byte{
		[]byte("index"),
		[]byte(collection),
		[]byte(index),
	}
	return bytes.Join(path, []byte("\x00"))
}

func collectionPrefix(collection string) []byte {
	path := [][]byte{
		[]byte("index"),
		[]byte(collection),
	}
	return bytes.Join(path, []byte("\x00"))
}
