package gokvkit

import (
	"context"
	"github.com/palantir/stacktrace"
	"github.com/spf13/cast"
)

// ConfigureCollection overwrites a single database collection configuration
func (d *DB) ConfigureCollection(ctx context.Context, collection CollectionConfig) error {
	meta, _ := GetMetadata(ctx)
	meta.Set("_internal", true)
	ctx = meta.ToContext(ctx)
	if collection.Name == "" {
		return stacktrace.NewError("a collection name is required")
	}
	if collection.Indexes == nil && len(collection.Indexes) < 1 {
		return stacktrace.NewError("%s: at least one collection is required", collection.Name)
	}
	var (
		hasPrimary  = 0
		primary     Index
		primaryName string
	)
	for name, v := range collection.Indexes {
		v.Collection = collection.Name
		v.Name = name
		if v.Primary {
			primary = v
			primaryName = name
			hasPrimary++
		}
		if name == "" {
			return stacktrace.NewError("%s: empty index name", collection.Name)
		}
		collection.Indexes[name] = v
	}
	if hasPrimary > 1 {
		return stacktrace.NewError("%s: only a single primary index is supported", collection.Name)
	}
	existing, _ := d.getPersistedCollection(collection.Name)

	if existing.Name == "" {
		existing.Name = collection.Name
		existing.Indexes = map[string]Index{}
	}

	var (
		existingHasPrimary = 0
	)

	for _, v := range existing.Indexes {
		if v.Primary {
			existingHasPrimary++
		}
	}
	switch {
	case existingHasPrimary > 1:
		return stacktrace.NewError("%s: only a single primary index is supported", collection.Name)
	case hasPrimary == 0 && existingHasPrimary == 0:
		return stacktrace.NewError("%s: a primary index is required", collection.Name)
	case existingHasPrimary == 0 && hasPrimary == 1:
		existing.Indexes[primaryName] = primary
		d.collections.Set(collection.Name, existing)
		if err := d.persistIndexes(collection.Name); err != nil {
			return stacktrace.Propagate(err, "")
		}
	}

	diff, err := getIndexDiff(collection.Indexes, existing.Indexes)
	if err != nil {
		return stacktrace.Propagate(err, "")
	}
	for _, update := range diff.toUpdate {
		if err := d.removeIndex(ctx, collection.Name, update); err != nil {
			return stacktrace.Propagate(err, "")
		}
		if err := d.addIndex(ctx, collection.Name, update); err != nil {
			return stacktrace.Propagate(err, "")
		}
	}
	for _, toDelete := range diff.toRemove {
		if err := d.removeIndex(ctx, collection.Name, toDelete); err != nil {
			return stacktrace.Propagate(err, "")
		}
	}
	for _, toAdd := range diff.toAdd {
		if err := d.addIndex(ctx, collection.Name, toAdd); err != nil {
			return stacktrace.Propagate(err, "")
		}
	}
	if err := d.persistIndexes(collection.Name); err != nil {
		return stacktrace.Propagate(err, "")
	}
	return nil
}

// Indexes returns a list of indexes that are registered in the collection
func (d *DB) Indexes(collection string) map[string]Index {
	return d.collections.Get(collection).Indexes
}

// Collections returns a list of collection names that are registered in the collection
func (d *DB) Collections() []string {
	var names []string
	d.collections.RangeR(func(key string, c CollectionConfig) bool {
		names = append(names, c.Name)
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
	indexes := d.collections.Get(collection).Indexes
	for _, index := range indexes {
		if index.Primary {
			return index
		}
	}
	return Index{}
}
