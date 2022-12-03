package gokvkit

import (
	"context"
	"github.com/palantir/stacktrace"
	"github.com/spf13/cast"
	"net/http"
)

// ConfigureCollection overwrites a single database collection configuration
func (d *DB) ConfigureCollection(ctx context.Context, collectionSchemaBytes []byte) error {
	meta, _ := GetMetadata(ctx)
	meta.Set("_internal", true)
	ctx = meta.ToContext(ctx)
	collection, err := newCollectionSchema(collectionSchemaBytes)

	var (
		hasPrimary = 0
		primary    Index
	)
	for _, v := range collection.indexing {
		v.Collection = collection.collection
		if v.Primary {
			primary = v
			hasPrimary++
		}
	}
	if hasPrimary > 1 {
		return stacktrace.NewError("%s: only a single primary index is supported", collection.collection)
	}
	existing, _ := d.getPersistedCollection(collection.collection)

	if existing == nil {
		existing = &collectionSchema{collection: collection.collection, indexing: map[string]Index{}}
	}

	var (
		existingHasPrimary = 0
	)

	for _, v := range existing.indexing {
		if v.Primary {
			existingHasPrimary++
		}
	}
	switch {
	case existingHasPrimary > 1:
		return stacktrace.NewError("%s: only a single primary index is supported", collection.collection)
	case hasPrimary == 0 && existingHasPrimary == 0:
		return stacktrace.NewError("%s: a primary index is required", collection.collection)
	case existingHasPrimary == 0 && hasPrimary == 1:
		existing.indexing[primary.Name] = primary
		d.collections.Set(collection.collection, existing)
		if err := d.persistIndexes(collection.collection); err != nil {
			return stacktrace.Propagate(err, "")
		}
	}

	diff, err := getIndexDiff(collection.indexing, existing.indexing)
	if err != nil {
		return stacktrace.Propagate(err, "")
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
	if err := d.persistIndexes(collection.collection); err != nil {
		return stacktrace.Propagate(err, "")
	}
	d.router.Set(collection.collection, "query", http.MethodPost, queryHandler(collection.collection, d))
	d.router.Set(collection.collection, "command", http.MethodPost, commandHandler(collection.collection, d))
	d.router.Set(collection.collection, "schema", http.MethodPut, schemaHandler(collection.collection, d))
	return nil
}

// Indexes returns a list of indexes that are registered in the collection
func (d *DB) Indexes(collection string) map[string]Index {
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

func (d *DB) getPrimaryKey(collection string, doc *Document) string {
	if d == nil {
		return ""
	}
	pkey := d.primaryKey(collection)
	return cast.ToString(doc.GetString(pkey))
}

func (d *DB) setPrimaryKey(collection string, doc *Document, id string) error {
	pkey := d.primaryKey(collection)
	return stacktrace.Propagate(doc.Set(pkey, id), "failed to set primary key")
}

func (d *DB) primaryIndex(collection string) Index {
	indexes := d.collections.Get(collection).indexing
	for _, index := range indexes {
		if index.Primary {
			return index
		}
	}
	return Index{}
}
