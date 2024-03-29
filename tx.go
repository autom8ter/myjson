package myjson

import (
	"context"
	"time"

	"github.com/autom8ter/myjson/errors"
	"github.com/autom8ter/myjson/kv"
	"github.com/autom8ter/myjson/util"
	"github.com/dop251/goja"
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
	vm      *goja.Runtime
	docs    map[string]struct{}
}

func (t *transaction) Commit(ctx context.Context) error {
	if err := t.tx.Commit(ctx); err != nil {
		return err
	}
	t.cdc = []CDC{}
	return nil
}

func (t *transaction) Rollback(ctx context.Context) error {
	if err := t.tx.Rollback(ctx); err != nil {
		return err
	}
	t.cdc = []CDC{}
	return nil
}

func (t *transaction) Update(ctx context.Context, collection string, id string, update map[string]any) error {
	schema, ctx := t.db.getSchema(ctx, collection)
	if schema == nil {
		return errors.New(errors.Validation, "tx: unsupported collection: %s", collection)
	}
	doc, err := NewDocumentFrom(update)
	if err != nil {
		return errors.Wrap(err, 0, "tx: failed to update")
	}
	if err := schema.SetPrimaryKey(doc, id); err != nil {
		return errors.Wrap(err, 0, "tx: failed to set primary key")
	}
	if err := t.persistCommand(ctx, &persistCommand{
		Collection: collection,
		Action:     UpdateAction,
		Document:   doc,
		Timestamp:  time.Now().UnixNano(),
		Metadata:   ExtractMetadata(ctx),
	}); err != nil {
		return errors.Wrap(err, 0, "tx: failed to commit update")
	}
	return nil
}

func (t *transaction) Create(ctx context.Context, collection string, document *Document) (string, error) {
	c, ctx := t.db.getSchema(ctx, collection)
	if c == nil {
		return "", errors.New(errors.Validation, "tx: unsupported collection: %s", collection)
	}
	var id = c.GetPrimaryKey(document)
	if id == "" {
		id = ksuid.New().String()
		err := c.SetPrimaryKey(document, id)
		if err != nil {
			return "", err
		}
	}
	if err := t.persistCommand(ctx, &persistCommand{
		Collection: collection,
		Action:     CreateAction,
		Document:   document,
		Timestamp:  time.Now().UnixNano(),
		Metadata:   ExtractMetadata(ctx),
	}); err != nil {
		return "", errors.Wrap(err, 0, "tx: failed to commit create")
	}
	return id, nil
}

func (t *transaction) Set(ctx context.Context, collection string, document *Document) error {
	schema, ctx := t.db.getSchema(ctx, collection)
	if schema == nil {
		return errors.New(errors.Validation, "tx: unsupported collection: %s", collection)
	}
	if err := t.persistCommand(ctx, &persistCommand{
		Collection: collection,
		Action:     SetAction,
		Document:   document,
		Timestamp:  time.Now().UnixNano(),
		Metadata:   ExtractMetadata(ctx),
	}); err != nil {
		return errors.Wrap(err, 0, "tx: failed to commit set")
	}
	return nil
}

func (t *transaction) Delete(ctx context.Context, collection string, id string) error {
	schema, ctx := t.db.getSchema(ctx, collection)
	if schema == nil {
		return errors.New(errors.Validation, "tx: unsupported collection: %s", collection)
	}
	d, _ := NewDocumentFrom(map[string]any{
		t.db.GetSchema(ctx, collection).PrimaryKey(): id,
	})
	if err := t.persistCommand(ctx, &persistCommand{
		Collection: collection,
		Action:     DeleteAction,
		Document:   d,
		Timestamp:  time.Now().UnixNano(),
		Metadata:   ExtractMetadata(ctx),
	}); err != nil {
		return errors.Wrap(err, 0, "tx: failed to commit delete")
	}
	return nil
}

func (t *transaction) Query(ctx context.Context, collection string, query Query) (Page, error) {
	if len(query.Select) == 0 {
		query.Select = append(query.Select, Select{Field: "*"})
	}
	if err := query.Validate(ctx); err != nil {
		return Page{}, err
	}
	schema, ctx := t.db.getSchema(ctx, collection)
	if schema == nil {
		return Page{}, errors.New(errors.Validation, "tx: unsupported collection: %s", collection)
	}
	allow, err := t.authorizeQuery(ctx, schema, &query)
	if err != nil {
		return Page{}, err
	}
	if !allow {
		return Page{}, errors.New(errors.Forbidden, "not authorized: %s/%s", collection, QueryAction)
	}
	if isAggregateQuery(query) {
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
			Explain:       &match,
		},
	}, nil
}

func (t *transaction) Get(ctx context.Context, collection string, id string) (*Document, error) {
	c, ctx := t.db.getSchema(ctx, collection)
	if c == nil {
		return nil, errors.New(errors.Validation, "tx: unsupported collection: %s", collection)
	}
	results, err := t.Query(ctx, collection, Query{Where: []Where{{Field: c.PrimaryKey(), Op: WhereOpEq, Value: id}}, Limit: 1})
	if err != nil {
		return nil, errors.Wrap(err, errors.NotFound, "%s not found", id)
	}
	if results.Count == 0 {
		return nil, errors.New(errors.NotFound, "%s not found", id)
	}
	return results.Documents[0], nil
}

func (t *transaction) Cmd(ctx context.Context, cmd TxCmd) TxResponse {
	switch {
	case cmd.Commit != nil:
		if err := t.Commit(ctx); err != nil {
			return TxResponse{Commit: &struct{}{}, Error: errors.Extract(err)}
		}
		return TxResponse{Commit: &struct{}{}}
	case cmd.Rollback != nil:
		if err := t.Rollback(ctx); err != nil {
			return TxResponse{Rollback: &struct{}{}, Error: errors.Extract(err)}
		}
		return TxResponse{Rollback: &struct{}{}}
	case cmd.Query != nil:
		results, err := t.Query(ctx, cmd.Query.Collection, cmd.Query.Query)
		if err != nil {
			return TxResponse{Error: errors.Extract(err)}
		}
		return TxResponse{
			Query: &results,
		}
	case cmd.Create != nil:
		_, err := t.Create(ctx, cmd.Create.Collection, cmd.Create.Document)
		if err != nil {
			return TxResponse{Error: errors.Extract(err)}
		}
		return TxResponse{
			Create: cmd.Create.Document,
		}
	case cmd.Set != nil:
		err := t.Set(ctx, cmd.Set.Collection, cmd.Set.Document)
		if err != nil {
			return TxResponse{Error: errors.Extract(err)}
		}
		return TxResponse{
			Set: cmd.Set.Document,
		}
	case cmd.Delete != nil:
		err := t.Delete(ctx, cmd.Delete.Collection, cmd.Delete.ID)
		if err != nil {
			return TxResponse{Error: errors.Extract(err)}
		}
		return TxResponse{
			Delete: &struct{}{},
		}
	case cmd.Get != nil:
		doc, err := t.Get(ctx, cmd.Get.Collection, cmd.Get.ID)
		if err != nil {
			return TxResponse{Error: errors.Extract(err)}
		}
		return TxResponse{
			Get: doc,
		}
	case cmd.Update != nil:
		err := t.Update(ctx, cmd.Update.Collection, cmd.Update.ID, cmd.Update.Update)
		if err != nil {
			return TxResponse{Error: errors.Extract(err)}
		}
		doc, err := t.Get(ctx, cmd.Update.Collection, cmd.Update.ID)
		if err != nil {
			return TxResponse{Error: errors.Extract(err)}
		}
		return TxResponse{
			Update: doc,
		}
	case cmd.TimeTravel != nil:
		doc, err := t.TimeTravel(ctx, cmd.TimeTravel.Collection, cmd.TimeTravel.ID, cmd.TimeTravel.Timestamp)
		if err != nil {
			return TxResponse{Error: errors.Extract(err)}
		}
		return TxResponse{
			TimeTravel: doc,
		}
	case cmd.Revert != nil:
		if err := t.Revert(ctx, cmd.Revert.Collection, cmd.Revert.ID, cmd.Revert.Timestamp); err != nil {
			return TxResponse{Error: errors.Extract(err)}
		}
		return TxResponse{
			Revert: &struct{}{},
		}
	}
	return TxResponse{Error: errors.Extract(errors.New(errors.Validation, "tx: unsupported command"))}
}

// aggregate performs aggregations against the collection
func (t *transaction) aggregate(ctx context.Context, collection string, query Query) (Page, error) {
	c, ctx := t.db.getSchema(ctx, collection)
	if c == nil {
		return Page{}, errors.New(errors.Validation, "tx: unsupported collection: %s", collection)
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
			Explain:       &match,
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

func (t *transaction) ForEach(ctx context.Context, collection string, opts ForEachOpts, fn ForEachFunc) (Explain, error) {
	pass, err := t.authorizeQuery(ctx, t.db.GetSchema(ctx, collection), &Query{
		Where: opts.Where,
		Join:  opts.Join,
	})
	if err != nil {
		return Explain{}, err
	}
	if !pass {
		return Explain{}, errors.New(errors.Forbidden, "not authorized: %s", QueryAction)
	}
	return t.queryScan(ctx, collection, opts.Where, opts.Join, fn)
}

func (t *transaction) Close(ctx context.Context) {
	t.tx.Close(ctx)
	t.cdc = []CDC{}
}

func (t *transaction) CDC() []CDC {
	return t.cdc
}

func (t *transaction) DB() Database {
	return t.db
}
