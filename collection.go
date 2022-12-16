package gokvkit

import (
	"context"

	"github.com/autom8ter/gokvkit/errors"
	"github.com/autom8ter/gokvkit/model"
	"github.com/spf13/cast"
)

// ConfigureCollection overwrites a single database collection configuration
func (d *DB) ConfigureCollection(ctx context.Context, collectionSchemaBytes []byte) error {
	meta, _ := model.GetMetadata(ctx)
	meta.Set(string(isIndexingKey), true)
	meta.Set(string(internalKey), true)
	ctx = meta.ToContext(ctx)
	collection, err := newCollectionSchema(collectionSchemaBytes)
	if err != nil {
		return err
	}
	for _, i := range collection.indexing {
		i.Collection = collection.collection
	}
	var (
		hasPrimary = 0
	)
	for _, v := range collection.indexing {
		v.Collection = collection.collection
		if v.Primary {
			hasPrimary++
		}
	}
	if hasPrimary > 1 {
		return errors.New(errors.Validation, "%s: only a single primary index is supported", collection.collection)
	}
	if hasPrimary == 0 {
		return errors.New(errors.Validation, "%s: a primary index is required", collection.collection)
	}
	if err := d.persistCollectionConfig(collection); err != nil {
		return err
	}

	existing, _ := d.getPersistedCollection(collection.collection)
	var diff indexDiff
	if existing == nil {
		diff, err = getIndexDiff(collection.indexing, map[string]model.Index{})
		if err != nil {
			return err
		}
	} else {
		diff, err = getIndexDiff(collection.indexing, existing.indexing)
		if err != nil {
			return err
		}
	}
	for _, update := range diff.toUpdate {
		if err := d.removeIndex(ctx, collection.collection, update); err != nil {
			return err
		}
		if err := d.addIndex(ctx, collection.collection, update); err != nil {
			return err
		}
	}
	for _, toDelete := range diff.toRemove {
		if err := d.removeIndex(ctx, collection.collection, toDelete); err != nil {
			return err
		}
	}
	for _, toAdd := range diff.toAdd {
		if err := d.addIndex(ctx, collection.collection, toAdd); err != nil {
			return err
		}
	}
	if err := d.persistCollectionConfig(collection); err != nil {
		return err
	}
	return nil
}

// Indexes returns a list of indexes that are registered in the collection
func (d *DB) Indexes(collection string) map[string]model.Index {
	return d.collections.Get(collection).indexing
}

// Collections returns a list of collection names that are registered in the collection
func (d *DB) Collections() []string {
	var names []string
	d.collections.Range(func(key string, c *collectionSchema) bool {
		names = append(names, c.collection)
		return true
	})
	return names
}

// PrimaryKey returns the collections primary key
func (d *DB) PrimaryKey(collection string) string {
	fields := d.primaryIndex(collection).Fields
	return fields[0]
}

func (d *DB) HasCollection(collection string) bool {
	return d.collections.Exists(collection)
}

func (d *DB) CollectionSchema(collection string) ([]byte, bool) {
	return d.collections.Get(collection).yamlRaw, d.HasCollection(collection)
}

func (d *DB) GetPrimaryKey(collection string, doc *model.Document) string {
	if d == nil {
		return ""
	}
	pkey := d.PrimaryKey(collection)
	return cast.ToString(doc.GetString(pkey))
}

func (d *DB) SetPrimaryKey(collection string, doc *model.Document, id string) error {
	pkey := d.PrimaryKey(collection)
	return errors.Wrap(doc.Set(pkey, id), 0, "failed to set primary key")
}

func (d *DB) primaryIndex(collection string) model.Index {
	indexes := d.collections.Get(collection).indexing
	for _, index := range indexes {
		if index.Primary {
			return index
		}
	}
	return model.Index{}
}
