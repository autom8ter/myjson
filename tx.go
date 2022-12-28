package gokvkit

import (
	"context"
	"time"

	"github.com/autom8ter/gokvkit/errors"
	"github.com/autom8ter/gokvkit/kv"
	"github.com/autom8ter/gokvkit/util"
	"github.com/samber/lo"
	"github.com/segmentio/ksuid"
)

// TxFunc is a function executed against a transaction - if the function returns an error, all changes will be rolled back.
// Otherwise, the changes will be commited to the database
type TxFunc func(ctx context.Context, tx Tx) error

// ForEachFunc returns false to stop scanning and an error if one occurred
type ForEachFunc func(d *Document) (bool, error)

type transaction struct {
	db      *defaultDB
	tx      kv.Tx
	isBatch bool
	cdc     []CDC
}

func (t *transaction) Commit(ctx context.Context) error {
	for _, h := range t.db.onCommit {
		if h.Before {
			if err := h.Func(ctx, t); err != nil {
				return err
			}
		}
	}
	for _, cdc := range t.cdc {
		t.db.cdcStream.Broadcast(ctx, cdc.Collection, cdc)
	}
	if err := t.tx.Commit(); err != nil {
		return err
	}
	for _, h := range t.db.onCommit {
		if !h.Before {
			if err := h.Func(ctx, t); err != nil {
				return err
			}
		}
	}
	t.cdc = []CDC{}
	return nil
}

func (t *transaction) Rollback(ctx context.Context) {
	for _, h := range t.db.onRollback {
		if h.Before {
			h.Func(ctx, t)
		}
	}
	t.tx.Rollback()
	for _, h := range t.db.onRollback {
		if !h.Before {
			h.Func(ctx, t)
		}
	}
	t.cdc = []CDC{}
}

func (t *transaction) Update(ctx context.Context, collection string, id string, update map[string]any) error {
	if !t.db.HasCollection(ctx, collection) {
		return errors.New(errors.Validation, "tx: unsupported collection: %s", collection)
	}
	doc, err := NewDocumentFrom(update)
	if err != nil {
		return errors.Wrap(err, 0, "tx: failed to update")
	}
	if err := t.db.GetSchema(ctx, collection).SetPrimaryKey(doc, id); err != nil {
		return errors.Wrap(err, 0, "tx: failed to set primary key")
	}
	md, _ := GetMetadata(ctx)
	if err := t.persistCommand(ctx, md, &Command{
		Collection: collection,
		Action:     Update,
		Document:   doc,
		Timestamp:  time.Now().UnixNano(),
		Metadata:   md,
	}); err != nil {
		return errors.Wrap(err, 0, "tx: failed to commit update")
	}
	return nil
}

func (t *transaction) Create(ctx context.Context, collection string, document *Document) (string, error) {
	if !t.db.HasCollection(ctx, collection) {
		return "", errors.New(errors.Validation, "unsupported collection: %s", collection)
	}
	var c = t.db.GetSchema(ctx, collection)
	var id = c.GetPrimaryKey(document)
	if id == "" {
		id = ksuid.New().String()
		err := c.SetPrimaryKey(document, id)
		if err != nil {
			return "", err
		}
	}
	md, _ := GetMetadata(ctx)
	if err := t.persistCommand(ctx, md, &Command{
		Collection: collection,
		Action:     Create,
		Document:   document,
		Timestamp:  time.Now().UnixNano(),
		Metadata:   md,
	}); err != nil {
		return "", errors.Wrap(err, 0, "tx: failed to commit create")
	}
	return id, nil
}

func (t *transaction) Set(ctx context.Context, collection string, document *Document) error {
	if !t.db.HasCollection(ctx, collection) {
		return errors.New(errors.Validation, "tx: unsupported collection: %s", collection)
	}
	md, _ := GetMetadata(ctx)
	if err := t.persistCommand(ctx, md, &Command{
		Collection: collection,
		Action:     Set,
		Document:   document,
		Timestamp:  time.Now().UnixNano(),
		Metadata:   md,
	}); err != nil {
		return errors.Wrap(err, 0, "tx: failed to commit set")
	}
	return nil
}

func (t *transaction) Delete(ctx context.Context, collection string, id string) error {
	if !t.db.HasCollection(ctx, collection) {
		return errors.New(errors.Validation, "tx: unsupported collection: %s", collection)
	}
	md, _ := GetMetadata(ctx)
	d, _ := NewDocumentFrom(map[string]any{
		t.db.GetSchema(ctx, collection).PrimaryKey(): id,
	})
	if err := t.persistCommand(ctx, md, &Command{
		Collection: collection,
		Action:     Delete,
		Document:   d,
		Timestamp:  time.Now().UnixNano(),
		Metadata:   md,
	}); err != nil {
		return errors.Wrap(err, 0, "tx: failed to commit delete")
	}
	return nil
}

func (t *transaction) Query(ctx context.Context, collection string, query Query) (Page, error) {
	if err := query.Validate(ctx); err != nil {
		return Page{}, err
	}
	if !t.db.HasCollection(ctx, collection) {
		return Page{}, errors.New(errors.Validation, "unsupported collection: %s", collection)
	}
	if query.IsAggregate() {
		return t.aggregate(ctx, collection, query)
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	now := time.Now()

	var results Documents
	fullScan := true
	match, err := t.queryScan(ctx, collection, query.Where, query.Join, func(d *Document) (bool, error) {
		results = append(results, d)
		if query.Page == 0 && len(query.OrderBy) == 0 && query.Limit > 0 && len(results) >= query.Limit {
			fullScan = false
			return false, nil
		}
		return true, nil
	})
	if err != nil {
		return Page{}, err
	}
	results = orderByDocs(results, query.OrderBy)

	if fullScan && query.Limit > 0 && query.Page > 0 {
		results = lo.Slice(results, query.Limit*query.Page, (query.Limit*query.Page)+query.Limit)
	}
	if query.Limit > 0 && len(results) > query.Limit {
		results = results[:query.Limit]
	}

	if len(query.Select) > 0 && query.Select[0].Field != "*" {
		for _, result := range results {
			err := selectDocument(result, query.Select)
			if err != nil {
				return Page{}, err
			}
		}
	}

	return Page{
		Documents: results,
		NextPage:  query.Page + 1,
		Count:     len(results),
		Stats: PageStats{
			ExecutionTime: time.Since(now),
			Optimization:  match,
		},
	}, nil
}

func (t *transaction) Get(ctx context.Context, collection string, id string) (*Document, error) {
	if !t.db.HasCollection(ctx, collection) {
		return nil, errors.New(errors.Validation, "unsupported collection: %s", collection)
	}
	md, _ := GetMetadata(ctx)
	md.Set(string(txCtx), t.tx)
	var c = t.db.GetSchema(ctx, collection)
	primaryIndex := c.PrimaryIndex()
	val, err := t.tx.Get(seekPrefix(collection, primaryIndex, map[string]any{
		c.PrimaryKey(): id,
	}).SetDocumentID(id).Path())
	if err != nil {
		return nil, errors.Wrap(err, errors.NotFound, "%s not found", id)
	}
	if val == nil {
		return nil, errors.New(errors.NotFound, "%s not found", id)
	}
	doc, err := NewDocumentFromBytes(val)
	if err != nil {
		return nil, err
	}
	if doc == nil {
		return nil, errors.New(errors.NotFound, "%s not found", id)
	}
	if doc == nil {
		return nil, errors.New(errors.NotFound, "%s not found", id)
	}
	return doc, nil
}

// aggregate performs aggregations against the collection
func (t *transaction) aggregate(ctx context.Context, collection string, query Query) (Page, error) {
	if !t.db.HasCollection(ctx, collection) {
		return Page{}, errors.New(errors.Validation, "unsupported collection: %s", collection)
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	now := time.Now()
	var results Documents
	match, err := t.queryScan(ctx, collection, query.Where, query.Join, func(d *Document) (bool, error) {
		results = append(results, d)
		return true, nil
	})
	if err != nil {
		return Page{}, err
	}
	var reduced Documents
	for _, values := range groupByDocs(results, query.GroupBy) {
		value, err := aggregateDocs(values, query.Select)
		if err != nil {
			return Page{}, err
		}
		reduced = append(reduced, value)
	}
	reduced, err = docsHaving(query.Having, reduced)
	if err != nil {
		return Page{}, errors.Wrap(err, errors.Internal, "")
	}
	reduced = orderByDocs(reduced, query.OrderBy)
	if query.Limit > 0 && query.Page > 0 {
		reduced = lo.Slice(reduced, query.Limit*query.Page, (query.Limit*query.Page)+query.Limit)
	}
	if query.Limit > 0 && len(reduced) > query.Limit {
		reduced = reduced[:query.Limit]
	}
	return Page{
		Documents: reduced,
		NextPage:  query.Page + 1,
		Count:     len(reduced),
		Stats: PageStats{
			ExecutionTime: time.Since(now),
			Optimization:  match,
		},
	}, nil
}

func docsHaving(where []Where, results Documents) (Documents, error) {
	if len(where) > 0 {
		for i, document := range results {
			pass, err := document.Where(where)
			if err != nil {
				return nil, err
			}
			if pass {
				results = util.RemoveElement(i, results)
			}
		}
	}
	return results, nil
}

func (t *transaction) ForEach(ctx context.Context, collection string, opts ForEachOpts, fn ForEachFunc) (Optimization, error) {
	if !t.db.HasCollection(ctx, collection) {
		return Optimization{}, errors.New(errors.Validation, "unsupported collection: %s", collection)
	}
	return t.queryScan(ctx, collection, opts.Where, opts.Join, fn)
}

func (t *transaction) Close(ctx context.Context) {
	t.tx.Close()
	t.cdc = []CDC{}
}

func (t *transaction) CDC() []CDC {
	return t.cdc
}

func (t *transaction) DB() Database {
	return t.db
}
