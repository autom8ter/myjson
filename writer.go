package wolverine

import (
	"context"
	"fmt"

	"github.com/autom8ter/machine/v4"
	"github.com/blevesearch/bleve"
	"github.com/dgraph-io/badger/v3"

	"github.com/autom8ter/wolverine/internal/prefix"
)

func (d *db) saveBatch(ctx context.Context, event *Event) error {
	if len(event.Documents) == 0 {
		return nil
	}
	if len(event.Documents) == 1 {
		return d.saveDocument(ctx, event)
	}
	collect, ok := d.getInmemCollection(event.Collection)
	if !ok {
		return d.wrapErr(fmt.Errorf("unsupported collection: %s", event.Collection), "")
	}
	for _, document := range event.Documents {
		if err := document.Validate(); err != nil {
			return d.wrapErr(err, "")
		}
		valid, err := collect.Validate(document)
		if err != nil {
			return d.wrapErr(err, "")
		}
		if !valid {
			return fmt.Errorf("%s/%s document has invalid schema", event.Collection, document.GetID())
		}
	}
	txn := d.kv.NewWriteBatch()
	var batch *bleve.Batch
	index := collect.fullText
	if index != nil {
		batch = index.NewBatch()
	}
	for _, document := range event.Documents {
		current, _ := d.Get(ctx, event.Collection, document.GetID())
		if current == nil {
			current = NewDocument()
		}
		var bits []byte
		switch event.Action {
		case Set:
			if err := document.Validate(); err != nil {
				return d.wrapErr(err, "")
			}
			bits = document.Bytes()
		case Update:
			currentClone := current.Clone()
			currentClone.Merge(document)
			bits = currentClone.Bytes()
		}
		for _, c := range d.config.Triggers {
			if err := c(ctx, event.Action, Before, current, document); err != nil {
				return d.wrapErr(err, "trigger failure")
			}
		}
		switch event.Action {
		case Set, Update:
			if err := txn.SetEntry(&badger.Entry{
				Key:   []byte(prefix.PrimaryKey(event.Collection, document.GetID())),
				Value: bits,
			}); err != nil {
				return d.wrapErr(err, "")
			}
			for _, index := range collect.Indexes {
				pindex := index.prefix(event.Collection)
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
		case Delete:
			for _, i := range collect.Indexes {
				pindex := i.prefix(event.Collection)
				if err := txn.Delete([]byte(pindex.GetIndex(current.Value()))); err != nil {
					return d.wrapErr(err, "")
				}
			}
			if err := txn.Delete([]byte(prefix.PrimaryKey(event.Collection, current.GetID()))); err != nil {
				return d.wrapErr(err, "")
			}
			if batch != nil {
				batch.Delete(document.GetID())
			}
		}
		for _, t := range d.config.Triggers {
			if err := t(ctx, event.Action, After, current, document); err != nil {
				return d.wrapErr(err, "trigger failure")
			}
		}
	}
	if index != nil {
		if err := index.Batch(batch); err != nil {
			return d.wrapErr(err, "")
		}
	}
	if err := txn.Flush(); err != nil {
		return d.wrapErr(err, "")
	}
	d.machine.Publish(ctx, machine.Message{
		Channel: event.Collection,
		Body:    event,
	})
	return nil
}

func (d *db) saveDocument(ctx context.Context, event *Event) error {
	collect, ok := d.getInmemCollection(event.Collection)
	if !ok {
		return d.wrapErr(fmt.Errorf("unsupported collection: %s", event.Collection), "")
	}
	if len(event.Documents) == 0 {
		return nil
	}
	if len(event.Documents) > 1 {
		return d.saveBatch(ctx, event)
	}
	document := event.Documents[0]
	if err := document.Validate(); err != nil {
		return d.wrapErr(err, "")
	}
	current, _ := d.Get(ctx, event.Collection, document.GetID())
	if current == nil {
		current = NewDocument()
	}

	var bits []byte
	switch event.Action {
	case Set:
		valid, err := collect.Validate(document)
		if err != nil {
			return d.wrapErr(err, "")
		}
		if !valid {
			return fmt.Errorf("%s/%s document has invalid schema", event.Collection, document.GetID())
		}
		bits = document.Bytes()
	case Update:
		currentClone := current.Clone()
		currentClone.Merge(document)
		if err := currentClone.Validate(); err != nil {
			return d.wrapErr(err, "")
		}
		valid, err := collect.Validate(currentClone)
		if err != nil {
			return d.wrapErr(err, "")
		}
		if !valid {
			return fmt.Errorf("%s/%s document has invalid schema", event.Collection, currentClone.GetID())
		}
		bits = currentClone.Bytes()
	}
	for _, t := range d.config.Triggers {
		if err := t(ctx, event.Action, Before, current, document); err != nil {
			return d.wrapErr(err, "trigger failure")
		}
	}
	return d.kv.Update(func(txn *badger.Txn) error {
		switch event.Action {
		case Set, Update:
			if err := txn.SetEntry(&badger.Entry{
				Key:   []byte(prefix.PrimaryKey(event.Collection, document.GetID())),
				Value: bits,
			}); err != nil {
				return d.wrapErr(err, "")
			}
			for _, index := range collect.Indexes {
				pindex := index.prefix(event.Collection)
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
			if collect.fullText != nil {
				if err := collect.fullText.Index(document.GetID(), document.Value()); err != nil {
					return d.wrapErr(err, "")
				}
			}
		case Delete:
			for _, index := range collect.Indexes {
				pindex := index.prefix(event.Collection)
				if err := txn.Delete([]byte(pindex.GetIndex(current.Value()))); err != nil {
					return d.wrapErr(err, "")
				}
			}
			if err := txn.Delete([]byte(prefix.PrimaryKey(event.Collection, current.GetID()))); err != nil {
				return d.wrapErr(err, "")
			}
			if collect.fullText != nil {
				if err := collect.fullText.Delete(document.GetID()); err != nil {
					return d.wrapErr(err, "")
				}
			}
		}
		for _, t := range d.config.Triggers {
			if err := t(ctx, event.Action, After, current, document); err != nil {
				return d.wrapErr(err, "trigger failure")
			}
		}
		d.machine.Publish(ctx, machine.Message{
			Channel: event.Collection,
			Body:    event,
		})
		return nil
	})
}

func (d *db) Set(ctx context.Context, collection string, document *Document) error {
	return d.saveDocument(ctx, &Event{
		Collection: collection,
		Action:     Set,
		Documents:  []*Document{document},
	})
}

func (d *db) BatchSet(ctx context.Context, collection string, batch []*Document) error {
	return d.saveBatch(ctx, &Event{
		Collection: collection,
		Action:     Set,
		Documents:  batch,
	})
}

func (d *db) Update(ctx context.Context, collection string, document *Document) error {
	return d.saveDocument(ctx, &Event{
		Collection: collection,
		Action:     Update,
		Documents:  []*Document{document},
	})
}

func (d *db) BatchUpdate(ctx context.Context, collection string, batch []*Document) error {
	return d.saveBatch(ctx, &Event{
		Collection: collection,
		Action:     Update,
		Documents:  batch,
	})
}

func (d *db) Delete(ctx context.Context, collection, id string) error {
	doc, err := d.Get(ctx, collection, id)
	if err != nil {
		return d.wrapErr(err, "")
	}
	return d.saveDocument(ctx, &Event{
		Collection: collection,
		Action:     Delete,
		Documents:  []*Document{doc},
	})
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

	return d.saveBatch(ctx, &Event{
		Collection: collection,
		Action:     Delete,
		Documents:  documents,
	})
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
