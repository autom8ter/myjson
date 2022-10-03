package wolverine

import (
	"context"
	"errors"
	"fmt"

	"github.com/autom8ter/machine/v4"
	"github.com/blevesearch/bleve"
	"github.com/dgraph-io/badger/v3"

	"github.com/autom8ter/wolverine/internal/prefix"
)

type mutation string

const (
	set    mutation = "set"
	update mutation = "update"
	del    mutation = "del"
)

func (d *db) saveBatch(ctx context.Context, collection string, mutation mutation, documents []*Document) error {
	if len(documents) == 0 {
		return nil
	}
	for _, document := range documents {
		if err := document.Validate(); err != nil {
			return d.wrapErr(err, "")
		}
		c, ok := d.collections[collection]
		if !ok {
			return fmt.Errorf("unsupported collection: %s", document.GetCollection())
		}
		valid, err := c.Validate(document)
		if err != nil {
			return d.wrapErr(err, "")
		}
		if !valid {
			return fmt.Errorf("%s/%s document has invalid schema", document.GetCollection(), document.GetID())
		}
	}
	collect := d.collections[collection]
	txn := d.kv.NewWriteBatch()
	var batch *bleve.Batch
	index := d.fullText[collection]
	if index != nil {
		batch = index.NewBatch()
	}
	for _, document := range documents {
		current, _ := d.Get(ctx, document.GetCollection(), document.GetID())
		if current == nil {
			current = NewDocument()
		}
		var bits []byte
		switch mutation {
		case set:
			document.SetCollection(collection)
			if err := document.Validate(); err != nil {
				return d.wrapErr(err, "")
			}
			bits = document.Bytes()
		case update:
			document.SetCollection(collection)
			if err := document.Validate(); err != nil {
				return d.wrapErr(err, "")
			}
			current.Merge(document)
			bits = current.Bytes()
		}
		switch mutation {
		case set, update:
			if err := txn.SetEntry(&badger.Entry{
				Key:   []byte(prefix.PrimaryKey(document.GetCollection(), document.GetID())),
				Value: bits,
			}); err != nil {
				return d.wrapErr(err, "")
			}
			for _, index := range collect.Indexes {
				pindex := index.prefix(document.GetCollection())
				if current != nil {
					if err := txn.Delete([]byte(pindex.GetIndex(current.Value()))); err != nil {
						return d.wrapErr(err, "")
					}
				}
				i := pindex.GetIndex(document.Value())
				if err := txn.SetEntry(&badger.Entry{
					Key:   []byte(i),
					Value: bits,
				}); err != nil {
					return d.wrapErr(err, "")
				}
			}
			if batch != nil {
				if err := batch.Index(document.GetID(), document.Value()); err != nil {
					return d.wrapErr(err, "")
				}
			}
		case del:
			for _, i := range collect.Indexes {
				pindex := i.prefix(current.GetCollection())
				if err := txn.Delete([]byte(pindex.GetIndex(current.Value()))); err != nil {
					return d.wrapErr(err, "")
				}
				if err := txn.Delete([]byte(prefix.PrimaryKey(current.GetCollection(), current.GetID()))); err != nil {
					return d.wrapErr(err, "")
				}
			}
			if batch != nil {
				batch.Delete(document.GetID())
			}
		}
	}
	if err := index.Batch(batch); err != nil {
		return d.wrapErr(err, "")
	}
	if err := txn.Flush(); err != nil {
		return d.wrapErr(err, "")
	}
	d.machine.Publish(ctx, machine.Message{
		Channel: collection,
		Body:    documents,
	})
	return nil
}

func (d *db) saveDocument(ctx context.Context, collection string, mutation mutation, document *Document) error {
	collect := d.collections[collection]
	current, _ := d.Get(ctx, document.GetCollection(), document.GetID())
	if current == nil {
		current = NewDocument()
	}
	var bits []byte
	switch mutation {
	case set:
		document.SetCollection(collection)
		if err := document.Validate(); err != nil {
			return d.wrapErr(err, "")
		}
		valid, err := collect.Validate(document)
		if err != nil {
			return d.wrapErr(err, "")
		}
		if !valid {
			return fmt.Errorf("%s/%s document has invalid schema", document.GetCollection(), document.GetID())
		}
		bits = document.Bytes()
	case update:
		document.SetCollection(collection)
		if err := document.Validate(); err != nil {
			return d.wrapErr(err, "")
		}
		current.Merge(document)
		valid, err := collect.Validate(current)
		if err != nil {
			return d.wrapErr(err, "")
		}
		if !valid {
			return fmt.Errorf("%s/%s document has invalid schema", current.GetCollection(), current.GetID())
		}
		bits = current.Bytes()
	default:
		return errors.New("invalid mutation")
	}
	return d.kv.Update(func(txn *badger.Txn) error {
		switch mutation {
		case set, update:
			if err := txn.SetEntry(&badger.Entry{
				Key:   []byte(prefix.PrimaryKey(document.GetCollection(), document.GetID())),
				Value: bits,
			}); err != nil {
				return d.wrapErr(err, "")
			}
			for _, index := range collect.Indexes {
				pindex := index.prefix(document.GetCollection())
				if current != nil {
					if err := txn.Delete([]byte(pindex.GetIndex(current.Value()))); err != nil {
						return d.wrapErr(err, "")
					}
				}
				i := pindex.GetIndex(document.Value())
				if err := txn.SetEntry(&badger.Entry{
					Key:   []byte(i),
					Value: bits,
				}); err != nil {
					return d.wrapErr(err, "")
				}
			}
			if index, ok := d.fullText[collection]; ok {
				if err := index.Index(document.GetID(), document.Value()); err != nil {
					return d.wrapErr(err, "")
				}
			}
		case del:
			for _, index := range collect.Indexes {
				pindex := index.prefix(current.GetCollection())
				if err := txn.Delete([]byte(pindex.GetIndex(current.Value()))); err != nil {
					return d.wrapErr(err, "")
				}
			}
			if index, ok := d.fullText[collection]; ok {
				if err := index.Delete(document.GetID()); err != nil {
					return d.wrapErr(err, "")
				}
			}
		}
		d.machine.Publish(ctx, machine.Message{
			Channel: collection,
			Body:    document,
		})
		return nil
	})
}

func (d *db) Set(ctx context.Context, collection string, document *Document) error {
	return d.saveDocument(ctx, collection, set, document)
}

func (d *db) BatchSet(ctx context.Context, collection string, batch []*Document) error {
	return d.saveBatch(ctx, collection, set, batch)
}

func (d *db) Update(ctx context.Context, collection string, document *Document) error {
	return d.saveDocument(ctx, collection, update, document)
}

func (d *db) BatchUpdate(ctx context.Context, collection string, batch []*Document) error {
	return d.saveBatch(ctx, collection, update, batch)
}

func (d *db) Delete(ctx context.Context, collection, id string) error {
	doc, err := d.Get(ctx, collection, id)
	if err != nil {
		return d.wrapErr(err, "")
	}
	return d.saveDocument(ctx, collection, del, doc)
}

func (d *db) BatchDelete(ctx context.Context, collection string, ids []string) error {
	var documents []*Document
	for _, id := range ids {
		doc, err := d.Get(ctx, collection, id)
		if err != nil {
			return d.wrapErr(err, "")
		}
		documents = append(documents, doc)
	}

	return d.saveBatch(ctx, collection, del, documents)
}

func (d *db) QueryUpdate(ctx context.Context, update *Document, collection string, query Query) error {
	documents, err := d.Query(ctx, collection, query)
	if err != nil {
		return d.wrapErr(err, "")
	}
	for _, document := range documents {
		document.Merge(update)
	}
	return d.BatchSet(ctx, collection, documents)
}

func (d *db) QueryDelete(ctx context.Context, collection string, query Query) error {
	documents, err := d.Query(ctx, collection, query)
	if err != nil {
		return d.wrapErr(err, "")
	}
	var ids []string
	for _, document := range documents {
		ids = append(ids, document.GetID())
	}
	return d.BatchDelete(ctx, collection, ids)
}
