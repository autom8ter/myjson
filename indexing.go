package gokvkit

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/autom8ter/gokvkit/internal/safe"
	"github.com/autom8ter/gokvkit/kv"
	"github.com/nqd/flat"
	"github.com/palantir/stacktrace"
	"reflect"
	"time"
)

// Index is a database index used to optimize queries against a collection
type Index struct {
	// Collection is the collection the index belongs to
	Collection string `json:"collection"`
	// Name is the indexes unique name in the collection
	Name string `json:"name"`
	// Fields to index - order matters
	Fields []string `json:"fields"`
	// Unique indicates that it's a unique index which will enforce uniqueness
	Unique bool `json:"unique"`
	// Unique indicates that it's a primary index
	Primary bool `json:"primary"`
	// IsBuilding indicates that the index is currently building
	IsBuilding bool `json:"isBuilding"`
}

// indexPrefix is a reference to a prefix within an index
type indexPrefix interface {
	// Append appends a field value to an index prefix
	Append(field string, value any) indexPrefix
	// Path returns the full path of the prefix
	Path() []byte
	// Fields returns the fields contained in the index prefix
	Fields() []FieldValue
	// DocumentID returns the document id set as the suffix of the prefix when Path() is called
	// This allows the index to seek to the position of an individual document
	DocumentID() string
	// SetDocumentID sets the document id as the suffix of the prefix when Path() is called
	// This allows the index to seek to the position of an individual document
	SetDocumentID(id string) indexPrefix
}

// FieldValue is a key value pair
type FieldValue struct {
	Field string `json:"field"`
	Value any    `json:"value"`
}

func (i Index) seekPrefix(fields map[string]any) indexPrefix {
	fields, _ = flat.Flatten(fields, nil)
	var prefix = indexPrefix(indexPathPrefix{
		prefix: [][]byte{
			[]byte("index"),
			[]byte(i.Collection),
			[]byte(i.Name),
		},
	})
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
	fieldMap   []FieldValue
}

func (p indexPathPrefix) Append(field string, value any) indexPrefix {
	fields := append(p.fields, []byte(field), encodeIndexValue(value))
	fieldMap := append(p.fieldMap, FieldValue{
		Field: field,
		Value: value,
	})
	return indexPathPrefix{
		prefix:   p.prefix,
		fields:   fields,
		fieldMap: fieldMap,
	}
}

func (p indexPathPrefix) SetDocumentID(id string) indexPrefix {
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

func (i indexPathPrefix) Fields() []FieldValue {
	return i.fieldMap
}

type indexDiff struct {
	toRemove []Index
	toAdd    []Index
	toUpdate []Index
}

func getIndexDiff(after, before map[string]Index) (indexDiff, error) {
	var (
		toRemove []Index
		toAdd    []Index
		toUpdate []Index
	)
	for _, index := range after {
		if _, ok := before[index.Name]; !ok {
			toAdd = append(toAdd, index)
		}
	}

	for _, current := range before {
		if _, ok := after[current.Name]; !ok {
			toRemove = append(toRemove, current)
		} else {
			if !reflect.DeepEqual(current.Fields, current.Fields) {
				toUpdate = append(toUpdate, current)
			}
		}
	}
	return indexDiff{
		toRemove: toRemove,
		toAdd:    toAdd,
		toUpdate: toUpdate,
	}, nil
}

func (d *DB) addIndex(ctx context.Context, collection string, index Index) error {
	if index.Name == "" {
		return stacktrace.NewError("%s - empty index name", collection)
	}
	if index.Collection == "" {
		index.Collection = collection
	}
	index.IsBuilding = true
	d.collections.SetFunc(collection, func(c CollectionConfig) CollectionConfig {
		c.Indexes[index.Name] = index
		return c
	})
	batch := d.kv.Batch()
	meta, _ := GetMetadata(ctx)
	meta.Set("_internal", true)
	meta.Set("_reindex", true)
	if !index.Primary {
		_, err := d.Scan(meta.ToContext(ctx), Scan{
			From:  collection,
			Where: nil,
		}, func(doc *Document) (bool, error) {
			if err := d.setDocument(ctx, batch, &Command{
				Collection: collection,
				Action:     SetDocument,
				DocID:      doc.GetString(d.primaryKey(collection)),
				Change:     doc,
			}); err != nil {
				return false, stacktrace.Propagate(err, "")
			}
			return true, nil
		})
		if err != nil {
			return stacktrace.Propagate(err, "%s - %s", collection, index.Name)
		}
	}
	if err := batch.Flush(); err != nil {
		return stacktrace.Propagate(err, "%s - %s", collection, index.Name)
	}
	index.IsBuilding = false
	d.collections.SetFunc(collection, func(c CollectionConfig) CollectionConfig {
		c.Indexes[index.Name] = index
		return c
	})
	return nil
}

func (d *DB) removeIndex(ctx context.Context, collection string, index Index) error {
	d.collections.SetFunc(collection, func(c CollectionConfig) CollectionConfig {
		delete(c.Indexes, index.Name)
		return c
	})
	batch := d.kv.Batch()
	meta, _ := GetMetadata(ctx)
	meta.Set("_internal", true)
	meta.Set("_reindex", true)
	_, err := d.queryScan(ctx, Scan{
		From: collection,
	}, func(doc *Document) (bool, error) {
		md, _ := GetMetadata(ctx)
		if err := d.updateSecondaryIndex(ctx, batch, index, &Command{
			Collection: collection,
			Action:     DeleteDocument,
			DocID:      doc.GetString(d.primaryKey(collection)),
			Before:     doc,
			Timestamp:  time.Now(),
			Metadata:   md,
		}); err != nil {
			return false, stacktrace.Propagate(err, "")
		}
		return true, nil
	})
	if err != nil {
		return stacktrace.Propagate(err, "")
	}
	return nil
}

func (d *DB) persistIndexes(collection string) error {
	val := d.collections.Get(collection)
	bits, err := json.Marshal(&val)
	if err != nil {
		return stacktrace.Propagate(err, "")
	}
	if err := d.kv.Tx(true, func(tx kv.Tx) error {
		err := tx.Set([]byte(fmt.Sprintf("internal.indexing.%s", collection)), bits)
		if err != nil {
			return stacktrace.Propagate(err, "")
		}
		return nil
	}); err != nil {
		return err
	}

	return nil
}

func (d *DB) getPersistedCollections() (*safe.Map[CollectionConfig], error) {
	var (
		collections = safe.NewMap(map[string]CollectionConfig{})
	)
	if err := d.kv.Tx(false, func(tx kv.Tx) error {
		i := tx.NewIterator(kv.IterOpts{
			Prefix: []byte("internal.indexing."),
		})
		defer i.Close()
		for i.Valid() {
			item := i.Item()
			var cfg CollectionConfig
			bits, err := item.Value()
			if err != nil {
				return stacktrace.Propagate(err, "")
			}
			err = json.Unmarshal(bits, &cfg)
			if err != nil {
				return stacktrace.Propagate(err, "")
			}
			collections.Set(cfg.Name, cfg)
			i.Next()
		}
		return nil
	}); err != nil {
		return nil, stacktrace.Propagate(err, "")
	}
	return collections, nil
}

func (d *DB) getPersistedCollection(collection string) (CollectionConfig, error) {
	var cfg CollectionConfig
	if err := d.kv.Tx(false, func(tx kv.Tx) error {
		bits, err := tx.Get([]byte(fmt.Sprintf("internal.indexing.%s", collection)))
		if err != nil {
			return stacktrace.Propagate(err, "")
		}
		err = json.Unmarshal(bits, &cfg)
		if err != nil {
			return stacktrace.Propagate(err, "")
		}
		return nil
	}); err != nil {
		return cfg, stacktrace.Propagate(err, "")
	}
	return cfg, nil
}
