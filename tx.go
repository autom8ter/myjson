package gokvkit

import (
	"context"
	"time"

	"github.com/autom8ter/gokvkit/errors"
	"github.com/autom8ter/gokvkit/internal/util"
	"github.com/autom8ter/gokvkit/kv"
	"github.com/autom8ter/gokvkit/model"
	"github.com/samber/lo"
	"github.com/segmentio/ksuid"
)

// Txn is a database transaction interface - it holds the methods used while using a transaction + commit,rollback,and close functionality
type Txn interface {
	// Commit commits the transaction to the database
	Commit(ctx context.Context) error
	// Rollback rollsback all changes made to the datbase
	Rollback(ctx context.Context)
	// Close closes the transaction - it should be deferred after
	Close(ctx context.Context)
	Tx
}

// Tx is a database transaction interface - it holds the methods used while using a transaction
type Tx interface {
	// Query executes a query against the database
	Query(ctx context.Context, collection string, query model.Query) (model.Page, error)
	// Get returns a document by id
	Get(ctx context.Context, collection string, id string) (*model.Document, error)
	// Create creates a new document - if the documents primary key is unset, it will be set as a sortable unique id
	Create(ctx context.Context, collection string, document *model.Document) (string, error)
	// Update updates a value in the database
	Update(ctx context.Context, collection, id string, document map[string]any) error
	// Set sets the specified key/value in the database
	Set(ctx context.Context, collection string, document *model.Document) error
	// Delete deletes the specified key from the database
	Delete(ctx context.Context, collection string, id string) error
	// Scan scans the optimal index for a collection's documents passing its filters.
	// results will not be ordered unless an index supporting the order by(s) was found by the optimizer
	// Query should be used when order is more important than performance/resource-usage
	Scan(ctx context.Context, scan model.Scan, handlerFunc model.ScanFunc) (model.OptimizerResult, error)
}

// TxFunc is a function executed against a transaction - if the function returns an error, all changes will be rolled back.
// Otherwise, the changes will be commited to the database
type TxFunc func(ctx context.Context, tx Tx) error

type transaction struct {
	db      *DB
	tx      kv.Tx
	isBatch bool
}

func (t *transaction) Commit(ctx context.Context) error {
	return t.tx.Commit()
}

func (t *transaction) Rollback(ctx context.Context) {
	t.tx.Rollback()
}

func (t *transaction) Update(ctx context.Context, collection string, id string, update map[string]any) error {
	if !t.db.HasCollection(collection) {
		return errors.New(errors.Validation, "tx: unsupported collection: %s", collection)
	}
	doc := model.NewDocument()
	if err := doc.SetAll(update); err != nil {
		return err
	}
	md, _ := model.GetMetadata(ctx)
	if err := t.persistCommand(ctx, md, &model.Command{
		Collection: collection,
		Action:     model.Update,
		DocID:      id,
		After:      doc,
		Timestamp:  time.Now(),
		Metadata:   md,
	}); err != nil {
		return errors.Wrap(err, 0, "tx: failed to commit update")
	}
	return nil
}

func (t *transaction) Create(ctx context.Context, collection string, document *model.Document) (string, error) {
	if !t.db.HasCollection(collection) {
		return "", errors.New(errors.Validation, "unsupported collection: %s", collection)
	}
	var c = t.db.collections.Get(collection)
	var id = c.GetPrimaryKey(document)
	if id == "" {
		id = ksuid.New().String()
		err := c.SetPrimaryKey(document, id)
		if err != nil {
			return "", err
		}
	}
	md, _ := model.GetMetadata(ctx)
	if err := t.persistCommand(ctx, md, &model.Command{
		Collection: collection,
		Action:     model.Create,
		DocID:      id,
		After:      document,
		Timestamp:  time.Now(),
		Metadata:   md,
	}); err != nil {
		return "", errors.Wrap(err, 0, "tx: failed to commit delete")
	}
	return id, nil
}

func (t *transaction) Set(ctx context.Context, collection string, document *model.Document) error {
	if !t.db.HasCollection(collection) {
		return errors.New(errors.Validation, "tx: unsupported collection: %s", collection)
	}
	md, _ := model.GetMetadata(ctx)
	if err := t.persistCommand(ctx, md, &model.Command{
		Collection: collection,
		Action:     model.Set,
		DocID:      t.db.collections.Get(collection).GetPrimaryKey(document),
		After:      document,
		Timestamp:  time.Now(),
		Metadata:   md,
	}); err != nil {
		return errors.Wrap(err, 0, "tx: failed to commit set")
	}
	return nil
}

func (t *transaction) Delete(ctx context.Context, collection string, id string) error {
	if !t.db.HasCollection(collection) {
		return errors.New(errors.Validation, "tx: unsupported collection: %s", collection)
	}
	md, _ := model.GetMetadata(ctx)
	if err := t.persistCommand(ctx, md, &model.Command{
		Collection: collection,
		Action:     model.Delete,
		DocID:      id,
		Timestamp:  time.Now(),
		Metadata:   md,
	}); err != nil {
		return errors.Wrap(err, 0, "tx: failed to commit delete")
	}
	return nil
}

func (t *transaction) Query(ctx context.Context, collection string, query model.Query) (model.Page, error) {
	if err := query.Validate(ctx); err != nil {
		return model.Page{}, err
	}
	if query.IsAggregate() {
		return t.aggregate(ctx, collection, query)
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	now := time.Now()

	if !t.db.HasCollection(collection) {
		return model.Page{}, errors.New(errors.Validation, "unsupported collection: %s", collection)
	}
	var results model.Documents
	fullScan := true
	match, err := t.queryScan(ctx, model.Scan{
		From:  collection,
		Where: query.Where,
	}, func(d *model.Document) (bool, error) {
		results = append(results, d)
		if query.Page != nil && *query.Page == 0 && len(query.OrderBy) == 0 && *query.Limit > 0 && len(results) >= *query.Limit {
			fullScan = false
			return false, nil
		}
		return true, nil
	})
	if err != nil {
		return model.Page{}, err
	}
	results = model.OrderByDocs(results, query.OrderBy)

	if fullScan && !util.IsNil(query.Limit) && !util.IsNil(query.Page) && *query.Limit > 0 && *query.Page > 0 {
		results = lo.Slice(results, *query.Limit**query.Page, (*query.Limit**query.Page)+*query.Limit)
	}
	if !util.IsNil(query.Limit) && *query.Limit > 0 && len(results) > *query.Limit {
		results = results[:*query.Limit]
	}

	if len(query.Select) > 0 && query.Select[0].Field != "*" {
		for _, result := range results {
			err := result.Select(query.Select)
			if err != nil {
				return model.Page{}, err
			}
		}
	}
	if query.Page == nil {
		query.Page = util.ToPtr(0)
	}
	return model.Page{
		Documents: results,
		NextPage:  *query.Page + 1,
		Count:     len(results),
		Stats: model.PageStats{
			ExecutionTime:   time.Since(now),
			OptimizerResult: match,
		},
	}, nil
}

func (t *transaction) Get(ctx context.Context, collection string, id string) (*model.Document, error) {
	if !t.db.HasCollection(collection) {
		return nil, errors.New(errors.Validation, "unsupported collection: %s", collection)
	}
	md, _ := model.GetMetadata(ctx)
	md.Set(string(txCtx), t.tx)
	var c = t.db.collections.Get(collection)
	primaryIndex := c.PrimaryIndex()
	val, err := t.tx.Get(primaryIndex.SeekPrefix(map[string]any{
		c.PrimaryKey(): id,
	}).SetDocumentID(id).Path())
	if err != nil {
		return nil, err
	}
	doc, err := model.NewDocumentFromBytes(val)
	if err != nil {
		return nil, err
	}
	doc, err = t.applyReadHooks(ctx, collection, doc)
	if err != nil {
		return doc, err
	}
	return doc, nil
}

// aggregate performs aggregations against the collection
func (t *transaction) aggregate(ctx context.Context, collection string, query model.Query) (model.Page, error) {
	if !t.db.HasCollection(collection) {
		return model.Page{}, errors.New(errors.Validation, "unsupported collection: %s", collection)
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	now := time.Now()
	var results model.Documents
	match, err := t.queryScan(ctx, model.Scan{
		From:  collection,
		Where: query.Where,
	}, func(d *model.Document) (bool, error) {
		results = append(results, d)
		return true, nil
	})
	if err != nil {
		return model.Page{}, err
	}
	var reduced model.Documents
	for _, values := range model.GroupByDocs(results, query.GroupBy) {
		value, err := model.AggregateDocs(values, query.Select)
		if err != nil {
			return model.Page{}, err
		}
		reduced = append(reduced, value)
	}
	reduced = model.OrderByDocs(reduced, query.OrderBy)
	if (!util.IsNil(query.Limit) && *query.Limit > 0) && (!util.IsNil(query.Limit) && *query.Page > 0) {
		reduced = lo.Slice(reduced, *query.Limit**query.Page, (*query.Limit**query.Page)+*query.Limit)
	}
	if !util.IsNil(query.Limit) && *query.Limit > 0 && len(reduced) > *query.Limit {
		reduced = reduced[:*query.Limit]
	}
	if query.Page == nil {
		query.Page = util.ToPtr(0)
	}
	return model.Page{
		Documents: reduced,
		NextPage:  *query.Page + 1,
		Count:     len(reduced),
		Stats: model.PageStats{
			ExecutionTime:   time.Since(now),
			OptimizerResult: match,
		},
	}, nil
}

func (t *transaction) Scan(ctx context.Context, scan model.Scan, handlerFunc model.ScanFunc) (model.OptimizerResult, error) {
	if !t.db.HasCollection(scan.From) {
		return model.OptimizerResult{}, errors.New(errors.Validation, "unsupported collection: %s", scan.From)
	}
	return t.queryScan(ctx, scan, handlerFunc)
}

func (t *transaction) Close(ctx context.Context) {
	t.tx.Close()
}
