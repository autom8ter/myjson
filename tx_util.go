package myjson

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/autom8ter/myjson/errors"
	"github.com/autom8ter/myjson/kv"
	"github.com/autom8ter/myjson/util"
	"github.com/nqd/flat"
	"github.com/samber/lo"
	"github.com/segmentio/ksuid"
	"github.com/spf13/cast"
	"golang.org/x/sync/errgroup"
)

func (t *transaction) updateDocument(ctx context.Context, c CollectionSchema, docID string, before *Document, command *persistCommand) error {
	if before == nil {
		return errors.New(errors.Internal, "tx: updateDocument - empty before value")
	}
	if c.Immutable() {
		return errors.New(errors.Forbidden, "tx: collection: %s is immutable", c.Collection())
	}
	for p, v := range c.PropertyPaths() {
		if v.Compute != nil && v.Compute.Write {
			val, err := t.vm.RunString(v.Compute.Expr)
			if err != nil {
				return errors.Wrap(err, errors.Internal, "failed to compute value")
			}
			if err := command.Document.Set(p, val.Export()); err != nil {
				return errors.Wrap(err, errors.Internal, "failed to set computed property: %s = %v", p, v.Compute)
			}
		}
		if v.Default != nil && !command.Document.Exists(p) {
			if err := command.Document.Set(p, v.Default); err != nil {
				return errors.Wrap(err, errors.Internal, "failed to set default property: %s = %v", p, v.Compute)
			}
		}
		if v.Immutable {
			if err := command.Document.Set(p, before.Get(p)); err != nil {
				return errors.Wrap(err, errors.Internal, "failed to set immutable property")
			}
		}
	}
	primaryIndex := c.PrimaryIndex()
	after := before.Clone()
	flattened, err := flat.Flatten(command.Document.Value(), nil)
	if err != nil {
		return err
	}
	err = after.SetAll(flattened)
	if err != nil {
		return err
	}
	if err := c.ValidateDocument(ctx, after); err != nil {
		return err
	}
	if err := t.tx.Set(ctx, seekPrefix(ctx, c.Collection(), primaryIndex, map[string]any{
		c.PrimaryKey(): docID,
	}).Seek(docID).Path(), after.Bytes()); err != nil {
		return errors.Wrap(err, errors.Internal, "failed to batch set documents to primary index")
	}
	return nil
}

func (t *transaction) deleteDocument(ctx context.Context, c CollectionSchema, docID string) error {
	if c.Immutable() {
		return errors.New(errors.Forbidden, "tx: collection: %s is immutable", c.Collection())
	}
	if c.PreventDeletes() {
		return errors.New(errors.Forbidden, "tx: collection: %s prevents deletes", c.Collection())
	}
	if docID == "" {
		return errors.New(errors.Validation, "tx: delete command - empty document id")
	}
	primaryIndex := c.PrimaryIndex()
	if err := t.tx.Delete(ctx, seekPrefix(ctx, c.Collection(), primaryIndex, map[string]any{
		c.PrimaryKey(): docID,
	}).Seek(docID).Path()); err != nil {
		return errors.Wrap(err, 0, "failed to delete documents")
	}
	return nil
}

func (t *transaction) createDocument(ctx context.Context, c CollectionSchema, command *persistCommand) error {
	primaryIndex := c.PrimaryIndex()
	docID := c.GetPrimaryKey(command.Document)
	if err := c.SetPrimaryKey(command.Document, docID); err != nil {
		return err
	}
	for p, v := range c.PropertyPaths() {
		if v.Compute != nil && v.Compute.Write {
			val, err := t.vm.RunString(cast.ToString(v.Compute.Expr))
			if err != nil {
				return errors.Wrap(err, errors.Internal, "failed to compute value")
			}
			if err := command.Document.Set(p, val.Export()); err != nil {
				return errors.Wrap(err, errors.Internal, "failed to compute property: %s = %v", p, v.Compute)
			}
		}
		if v.Default != nil && !command.Document.Exists(p) {
			if err := command.Document.Set(p, v.Default); err != nil {
				return errors.Wrap(err, errors.Internal, "failed to set default property: %s = %v", p, v.Default)
			}
		}
	}
	if err := c.ValidateDocument(ctx, command.Document); err != nil {
		return err
	}
	if err := t.tx.Set(ctx, seekPrefix(ctx, c.Collection(), primaryIndex, map[string]any{
		c.PrimaryKey(): docID,
	}).Seek(docID).Path(), command.Document.Bytes()); err != nil {
		return errors.Wrap(err, errors.Internal, "failed to batch set documents to primary index")
	}
	return nil
}

func (t *transaction) setDocument(ctx context.Context, c CollectionSchema, docID string, before *Document, command *persistCommand) error {
	if docID == "" {
		return errors.New(errors.Validation, "tx: set command - empty document id")
	}
	if c.Immutable() && before != nil {
		return errors.New(errors.Forbidden, "tx: collection: %s is immutable", command.Collection)
	}
	for p, v := range c.PropertyPaths() {
		if v.Compute != nil && v.Compute.Write {
			val, err := t.vm.RunString(v.Compute.Expr)
			if err != nil {
				return errors.Wrap(err, errors.Internal, "failed to compute value")
			}
			if err := command.Document.Set(p, val.Export()); err != nil {
				return errors.Wrap(err, errors.Internal, "failed to compute property: %s = %v", p, v.Compute)
			}
		}
		if v.Default != nil && !command.Document.Exists(p) {
			if err := command.Document.Set(p, v.Default); err != nil {
				return errors.Wrap(err, errors.Internal, "failed to set default property: %s = %v", p, v.Default)
			}
		}
		if before != nil && v.Immutable && command.Document.Get(p) != before.Get(p) {
			if err := command.Document.Set(p, before.Get(p)); err != nil {
				return errors.Wrap(err, errors.Internal, "failed to set immutable property")
			}
		}
	}
	if err := c.ValidateDocument(ctx, command.Document); err != nil {
		return err
	}
	primaryIndex := c.PrimaryIndex()
	if err := t.tx.Set(ctx, seekPrefix(ctx, c.Collection(), primaryIndex, map[string]any{
		c.PrimaryKey(): docID,
	}).Seek(docID).Path(), command.Document.Bytes()); err != nil {
		return errors.Wrap(err, errors.Internal, "failed to set documents to primary index")
	}
	return nil
}

func (t *transaction) persistCommand(ctx context.Context, command *persistCommand) error {
	c := t.db.GetSchema(ctx, command.Collection)
	if c == nil {
		return fmt.Errorf("tx: collection: %s does not exist", command.Collection)
	}
	if c.IsReadOnly() && !isInternal(ctx) {
		return fmt.Errorf("tx: collection: %s is read only", command.Collection)
	}

	docID := c.GetPrimaryKey(command.Document)
	if command.Timestamp == 0 {
		command.Timestamp = time.Now().UnixNano()
	}
	if command.Metadata == nil {
		command.Metadata = ExtractMetadata(ctx)
	}
	if err := util.ValidateStruct(c); err != nil {
		return err
	}
	before, _ := t.Get(ctx, command.Collection, docID)
	if isIndexing(ctx) {
		for _, i := range c.Indexing() {
			if i.Primary {
				continue
			}
			if err := t.updateSecondaryIndex(ctx, c, i, docID, before, command); err != nil {
				return errors.Wrap(err, errors.Internal, "")
			}
		}
		return nil
	}
	//if t.db.collectionIsLocked(ctx, command.Collection) {
	//	return errors.New(errors.Forbidden, "collection %s is locked", command.Collection)
	//}
	if err := t.evaluate(ctx, c, command); err != nil {
		return err
	}

	switch command.Action {
	case UpdateAction:
		if c.Immutable() {
			return errors.New(errors.Forbidden, "tx: collection: %s is immutable", command.Collection)
		}
		if err := t.updateDocument(ctx, c, docID, before, command); err != nil {
			return err
		}
		t.docs[fmt.Sprintf("%s/%s", command.Collection, docID)] = struct{}{}
	case CreateAction:
		if err := t.createDocument(ctx, c, command); err != nil {
			return err
		}
		t.docs[fmt.Sprintf("%s/%s", command.Collection, docID)] = struct{}{}
	case DeleteAction:
		if err := t.deleteDocument(ctx, c, docID); err != nil {
			return err
		}
		if err := t.cascadeDelete(ctx, c, command); err != nil {
			return err
		}
		delete(t.docs, fmt.Sprintf("%s/%s", command.Collection, docID))
	case SetAction:
		if err := t.setDocument(ctx, c, docID, before, command); err != nil {
			return err
		}
		t.docs[fmt.Sprintf("%s/%s", command.Collection, docID)] = struct{}{}
	default:
		return fmt.Errorf("tx: unsupported action: %s", command.Action)
	}
	for _, i := range c.Indexing() {
		i := i
		if err := t.updateSecondaryIndex(ctx, c, i, docID, before, command); err != nil {
			return errors.Wrap(err, 0, "failed to update secondary index")
		}
	}
	if command.Collection != cdcCollectionName {
		cdc := CDC{
			ID:         ksuid.New().String(),
			Collection: command.Collection,
			Action:     command.Action,
			DocumentID: c.GetPrimaryKey(command.Document),
			Timestamp:  command.Timestamp,
			Metadata:   command.Metadata,
			Diff:       command.Document.Diff(before),
		}
		if cdc.Diff == nil {
			cdc.Diff = []JSONFieldOp{}
		}
		cdcDoc, err := NewDocumentFrom(&cdc)
		if err != nil {
			return errors.Wrap(err, errors.Internal, "failed to persist cdc")
		}

		ctx = context.WithValue(ctx, internalKey, true)
		if err := t.persistCommand(ctx, &persistCommand{
			Collection: "system_cdc",
			Action:     CreateAction,
			Document:   cdcDoc,
			Timestamp:  cdc.Timestamp,
			Metadata:   cdc.Metadata,
		}); err != nil {
			return errors.Wrap(err, errors.Internal, "failed to persist cdc")
		}
		t.cdc = append(t.cdc, cdc)
	}
	return nil
}

func (t *transaction) cascadeDelete(ctx context.Context, schema CollectionSchema, command *persistCommand) error {
	egp, ctx := errgroup.WithContext(ctx)
	for _, c := range t.db.getCachedCollections() {
		c := c
		egp.Go(func() error {
			for _, i := range c.Indexing() {
				if i.ForeignKey != nil && i.ForeignKey.Collection == command.Collection {
					results, err := t.Query(ctx, c.Collection(), Query{
						Select: []Select{{Field: "*"}},
						Where: []Where{
							{
								Field: i.Fields[0],
								Op:    WhereOpEq,
								Value: schema.GetPrimaryKey(command.Document),
							},
						},
					})
					if err != nil {
						return err
					}
					for _, d := range results.Documents {
						if err := t.Delete(ctx, c.Collection(), c.GetPrimaryKey(d)); err != nil {
							return err
						}
					}
				}
			}
			return nil
		})
	}
	if err := egp.Wait(); err != nil {
		return errors.Wrap(err, errors.Internal, "%s failed to cascade delete reference documents", schema.Collection())
	}
	return nil
}

func (t *transaction) updateSecondaryIndex(ctx context.Context, schema CollectionSchema, idx Index, docID string, before *Document, command *persistCommand) error {
	if idx.Primary {
		return nil
	}
	switch command.Action {
	case DeleteAction:
		if err := t.tx.Delete(ctx, seekPrefix(ctx, command.Collection, idx, before.Value()).Seek(docID).Path()); err != nil {
			return errors.Wrap(
				err,
				errors.Internal,
				"failed to delete document %s/%s index references",
				command.Collection,
				docID,
			)
		}
		delete(t.docs, fmt.Sprintf("%s/%s", command.Collection, docID))
	case SetAction, UpdateAction, CreateAction:
		if before != nil {
			if err := t.tx.Delete(ctx, seekPrefix(ctx, command.Collection, idx, before.Value()).Seek(docID).Path()); err != nil {
				return errors.Wrap(
					err,
					errors.Internal,
					"failed to delete document %s/%s index references",
					command.Collection,
					docID,
				)
			}
		}
		if idx.ForeignKey != nil && command.Document.Get(idx.Fields[0]) != nil {
			fcollection, ctx := t.db.getSchema(ctx, idx.ForeignKey.Collection)
			if fcollection == nil {
				return errors.New(errors.Validation, "foreign_key collection does not exist: %s", idx.ForeignKey.Collection)
			}
			has, err := t.hasDocID(ctx, fcollection, command.Document.GetString(idx.Fields[0]))
			if err != nil {
				return errors.Wrap(err, errors.Internal, "")
			}
			if !has {
				return errors.New(errors.Validation, "foreign key with value %v does not exist: %s/%s",
					command.Document.Get(idx.Fields[0]),
					idx.ForeignKey.Collection,
					fcollection.PrimaryKey(),
				)
			}
		}
		if idx.Unique && !idx.Primary && command.Document != nil {
			it, err := t.tx.NewIterator(kv.IterOpts{
				Prefix: seekPrefix(ctx, command.Collection, idx, command.Document.Value()).Path(),
			})
			if err != nil {
				return err
			}
			defer it.Close()
			for it.Valid() {
				split := bytes.Split(it.Key(), []byte("\x00"))
				id := split[len(split)-1]
				if string(id) != docID {
					return errors.New(errors.Validation, "duplicate value( %s ) found for unique index: %s", docID, idx.Name)
				}
				if err := it.Next(); err != nil {
					return err
				}
			}
		}
		// only persist ids in secondary index - lookup full document in primary index
		if err := t.tx.Set(ctx, seekPrefix(ctx, command.Collection, idx, command.Document.Value()).Seek(docID).Path(), []byte(docID)); err != nil {
			return errors.Wrap(
				err,
				errors.Internal,
				"failed to set document %s/%s index references",
				command.Collection,
				docID,
			)
		}
		t.docs[fmt.Sprintf("%s/%s", command.Collection, docID)] = struct{}{}
	}
	return nil
}

func (t *transaction) queryScan(ctx context.Context, collection string, where []Where, join []Join, fn ForEachFunc) (Explain, error) {
	if fn == nil {
		return Explain{}, errors.New(errors.Validation, "empty scan handler")
	}
	c, ctx := t.db.getSchema(ctx, collection)
	if c == nil {
		return Explain{}, errors.New(errors.Validation, "tx: unsupported collection: %s", collection)
	}
	var computed = map[string]*ComputedField{}
	for p, v := range c.PropertyPaths() {
		if v.Compute != nil && v.Compute.Read {
			computed[p] = v.Compute
		}
	}
	var err error
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	//if t.db.collectionIsLocked(ctx, collection) {
	//	return Explain{}, errors.New(errors.Forbidden, "collection %s is locked", collection)
	//}
	explain, err := t.db.optimizer.Optimize(t.db.GetSchema(ctx, collection), where)
	if err != nil {
		return Explain{}, err
	}

	pfx := seekPrefix(ctx, collection, explain.Index, explain.MatchedValues)
	opts := kv.IterOpts{
		Prefix:  pfx.Path(),
		Reverse: explain.Reverse,
	}
	if explain.SeekFields != nil {
		for _, field := range explain.SeekFields {
			pfx = pfx.Append(field, explain.SeekValues[field])
		}
		opts.Seek = pfx.Path()
	} else {
		opts.Seek = opts.Prefix
	}
	it, err := t.tx.NewIterator(opts)
	if err != nil {
		return Explain{}, err
	}
	defer it.Close()
	for it.Valid() {
		var document *Document
		if explain.Index.Primary {
			bits, err := it.Value()
			if err != nil {
				return explain, err
			}
			document, err = NewDocumentFromBytes(bits)
			if err != nil {
				return explain, err
			}
		} else {
			split := bytes.Split(it.Key(), []byte("\x00"))
			id := split[len(split)-1]
			document, err = t.Get(ctx, collection, string(id))
			if err != nil {
				return explain, err
			}
		}
		for p, c := range computed {
			val, err := t.vm.RunString(c.Expr)
			if err != nil {
				return explain, errors.Wrap(err, errors.Internal, "failed to compute field %s", p)
			}
			if err := document.Set(p, val.Export()); err != nil {
				return explain, err
			}
		}
		var documents = []*Document{document}
		if len(join) > 0 {

			for _, j := range join {
				alias := j.As
				if alias == "" {
					alias = j.Collection
				}
				var newJoin = Join{
					Collection: j.Collection,
					On:         nil,
					As:         alias,
				}
				for i, o := range j.On {
					if strings.HasPrefix(cast.ToString(o.Value), selfRefPrefix) {
						newJoin.On = append(newJoin.On, Where{
							Field: j.On[i].Field,
							Op:    j.On[i].Op,
							Value: documents[0].Get(strings.TrimPrefix(cast.ToString(o.Value), selfRefPrefix)),
						})
					} else {
						newJoin.On = append(newJoin.On, o)
					}
				}
				results, err := t.Query(ctx, j.Collection, Query{
					Select: []Select{{Field: "*"}},
					Join:   nil,
					Where:  newJoin.On,
				})
				if err != nil {
					return Explain{}, err
				}
				for i, d := range results.Documents {
					if len(documents) > i {
						if err := documents[i].MergeJoin(d, j.As); err != nil {
							return Explain{}, err
						}
					} else {
						cloned := documents[0].Clone()
						if err := cloned.MergeJoin(d, j.As); err != nil {
							return Explain{}, err
						}
						documents = append(documents, cloned)
					}

				}
			}
		}
		for _, d := range documents {
			pass, err := d.Where(where)
			if err != nil {
				return Explain{}, err
			}
			if pass {
				shouldContinue, err := fn(d)
				if err != nil {
					return Explain{}, err
				}
				if !shouldContinue {
					return Explain{}, nil
				}
			}
		}
		if err := it.Next(); err != nil {
			return Explain{}, err
		}
	}
	return explain, nil
}

func (t *transaction) evaluate(ctx context.Context, c CollectionSchema, command *persistCommand) error {
	if err := t.vm.Set(string(JavascriptGlobalCtx), ctx); err != nil {
		return err
	}
	if err := t.vm.Set(string(JavascriptGlobalDoc), command.Document); err != nil {
		return err
	}
	if err := t.vm.Set(string(JavascriptGlobalMeta), command.Metadata); err != nil {
		return err
	}
	if err := t.vm.Set(string(JavascriptGlobalSchema), c); err != nil {
		return err
	}

	for _, trigger := range c.Triggers() {
		trigger.Script = t.db.globalScripts + trigger.Script
		switch {
		case command.Action == DeleteAction && lo.Contains(trigger.Events, OnDelete):
			if _, err := t.vm.RunString(trigger.Script); err != nil {
				return err
			}
		case command.Action == SetAction && lo.Contains(trigger.Events, OnSet):
			if _, err := t.vm.RunString(trigger.Script); err != nil {
				return err
			}
		case command.Action == CreateAction && lo.Contains(trigger.Events, OnCreate):
			if _, err := t.vm.RunString(trigger.Script); err != nil {
				return err
			}
		case command.Action == UpdateAction && lo.Contains(trigger.Events, OnUpdate):
			if _, err := t.vm.RunString(trigger.Script); err != nil {
				return err
			}
		}
	}
	if !isInternal(ctx) {
		pass, err := t.authorizeCommand(ctx, c, command)
		if err != nil {
			return err
		}
		if !pass {
			return errors.New(errors.Forbidden, "not authorized: %s", command.Action)
		}
	}

	return nil
}

func (t *transaction) hasDocID(ctx context.Context, schema CollectionSchema, id string) (bool, error) {
	if _, ok := t.docs[fmt.Sprintf("%s/%s", schema.Collection(), id)]; ok {
		return true, nil
	}
	results, err := t.Query(ctx, schema.Collection(), Query{
		Select: []Select{{Field: "*"}},
		Where: []Where{
			{
				Field: schema.PrimaryKey(),
				Op:    WhereOpEq,
				Value: id,
			},
		},
		Limit: 1,
	})
	if err != nil {
		return false, errors.Wrap(err, errors.Internal, "")
	}
	if results.Count == 0 {
		return false, errors.New(errors.Validation, "foreign key with value %v does not exist: %s/%s",
			id,
			schema.Collection(),
			schema.PrimaryKey(),
		)
	}
	t.docs[fmt.Sprintf("%s/%s", schema.Collection(), id)] = struct{}{}
	return true, nil
}

func (t *transaction) TimeTravel(ctx context.Context, collection string, documentID string, timestamp time.Time) (*Document, error) {
	current, err := t.Get(ctx, collection, documentID)
	if err != nil {
		return nil, err
	}
	if current == nil {
		return nil, errors.New(errors.NotFound, "document not found: %s/%s", collection, documentID)
	}
	results, err := t.Query(ctx, cdcCollectionName, Query{
		Where: []Where{
			{
				Field: "documentID",
				Op:    WhereOpEq,
				Value: documentID,
			},
			{
				Field: "collection",
				Op:    WhereOpEq,
				Value: collection,
			},
			{
				Field: "timestamp",
				Op:    WhereOpGte,
				Value: timestamp.UnixNano(),
			},
		},
		OrderBy: []OrderBy{
			{
				Field:     "timestamp",
				Direction: OrderByDirectionDesc,
			},
		},
	})
	if err != nil {
		return nil, err
	}
	if len(results.Documents) == 0 {
		return nil, errors.New(errors.NotFound, "not found: %s/%s", collection, documentID)
	}
	for _, doc := range results.Documents {
		var cdc CDC
		if err := doc.Scan(&cdc); err != nil {
			return nil, err
		}
		if err := current.RevertOps(cdc.Diff); err != nil {
			return nil, err
		}
	}

	return current, nil
}

func (t *transaction) Revert(ctx context.Context, collection string, documentID string, timestamp time.Time) error {
	document, err := t.TimeTravel(ctx, collection, documentID, timestamp)
	if err != nil {
		return err
	}
	return t.Set(ctx, collection, document)
}
