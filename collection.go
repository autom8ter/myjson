package gokvkit

import (
	"context"

	"github.com/autom8ter/gokvkit/model"
	"github.com/palantir/stacktrace"
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
		return stacktrace.Propagate(err, "")
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
		return stacktrace.NewError("%s: only a single primary index is supported", collection.collection)
	}
	if hasPrimary == 0 {
		return stacktrace.NewError("%s: a primary index is required", collection.collection)
	}
	if err := d.persistCollectionConfig(collection); err != nil {
		return stacktrace.Propagate(err, "")
	}

	existing, _ := d.getPersistedCollection(collection.collection)
	var diff indexDiff
	if existing == nil {
		diff, err = getIndexDiff(collection.indexing, map[string]model.Index{})
		if err != nil {
			return stacktrace.Propagate(err, "")
		}
	} else {
		diff, err = getIndexDiff(collection.indexing, existing.indexing)
		if err != nil {
			return stacktrace.Propagate(err, "")
		}
	}
	for _, update := range diff.toUpdate {
		if err := d.removeIndex(ctx, collection.collection, update); err != nil {
			return stacktrace.Propagate(err, "")
		}
		if err := d.addIndex(ctx, collection.collection, update); err != nil {
			return stacktrace.Propagate(err, "")
		}
	}
	for _, toDelete := range diff.toRemove {
		if err := d.removeIndex(ctx, collection.collection, toDelete); err != nil {
			return stacktrace.Propagate(err, "")
		}
	}
	for _, toAdd := range diff.toAdd {
		if err := d.addIndex(ctx, collection.collection, toAdd); err != nil {
			return stacktrace.Propagate(err, "")
		}
	}
	if err := d.persistCollectionConfig(collection); err != nil {
		return stacktrace.Propagate(err, "")
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
	d.collections.RangeR(func(key string, c *collectionSchema) bool {
		names = append(names, c.collection)
		return true
	})
	return names
}

// PrimaryKey returns the collections primary key
func (d *DB) primaryKey(collection string) string {
	fields := d.primaryIndex(collection).Fields
	return fields[0]
}

func (d *DB) hasCollection(collection string) bool {
	return d.collections.Exists(collection)
}

func (d *DB) getCollectionSchema(collection string) ([]byte, bool) {
	return []byte(d.collections.Get(collection).raw.Raw), d.hasCollection(collection)
}

func (d *DB) getPrimaryKey(collection string, doc *model.Document) string {
	if d == nil {
		return ""
	}
	pkey := d.primaryKey(collection)
	return cast.ToString(doc.GetString(pkey))
}

func (d *DB) setPrimaryKey(collection string, doc *model.Document, id string) error {
	pkey := d.primaryKey(collection)
	return stacktrace.Propagate(doc.Set(pkey, id), "failed to set primary key")
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
