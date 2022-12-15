package gokvkit

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/autom8ter/gokvkit/errors"
	"github.com/autom8ter/gokvkit/kv"
	"github.com/autom8ter/gokvkit/model"
	"github.com/nqd/flat"
	"github.com/segmentio/ksuid"
)

func (t *transaction) updateDocument(ctx context.Context, c *collectionSchema, command *model.Command) error {
	primaryIndex := t.db.primaryIndex(command.Collection)
	after := command.Before.Clone()
	flattened, err := flat.Flatten(command.After.Value(), nil)
	if err != nil {
		return err
	}
	err = after.SetAll(flattened)
	if err != nil {
		return err
	}
	command.After = after
	if err := c.validateCommand(ctx, command); err != nil {
		return err
	}
	if err := t.tx.Set(primaryIndex.SeekPrefix(map[string]any{
		t.db.PrimaryKey(command.Collection): command.DocID,
	}).SetDocumentID(command.DocID).Path(), command.After.Bytes()); err != nil {
		return errors.Wrap(err, errors.Internal, "failed to batch set documents to primary index")
	}
	return nil
}

func (t *transaction) deleteDocument(ctx context.Context, c *collectionSchema, command *model.Command) error {
	if err := c.validateCommand(ctx, command); err != nil {
		return err
	}
	primaryIndex := t.db.primaryIndex(command.Collection)
	if err := t.tx.Delete(primaryIndex.SeekPrefix(map[string]any{
		t.db.PrimaryKey(command.Collection): command.DocID,
	}).SetDocumentID(command.DocID).Path()); err != nil {
		return errors.Wrap(err, 0, "failed to batch delete documents")
	}
	return nil
}

func (t *transaction) createDocument(ctx context.Context, c *collectionSchema, command *model.Command) error {
	primaryIndex := t.db.primaryIndex(command.Collection)
	if command.DocID == "" {
		command.DocID = ksuid.New().String()
		if err := t.db.SetPrimaryKey(command.Collection, command.After, command.DocID); err != nil {
			return err
		}
	}
	if err := c.validateCommand(ctx, command); err != nil {
		return err
	}
	if err := t.tx.Set(primaryIndex.SeekPrefix(map[string]any{
		t.db.PrimaryKey(command.Collection): command.DocID,
	}).SetDocumentID(command.DocID).Path(), command.After.Bytes()); err != nil {
		return errors.Wrap(err, errors.Internal, "failed to batch set documents to primary index")
	}

	return nil
}

func (t *transaction) setDocument(ctx context.Context, c *collectionSchema, command *model.Command) error {
	if err := c.validateCommand(ctx, command); err != nil {
		return err
	}
	primaryIndex := t.db.primaryIndex(command.Collection)
	if err := t.tx.Set(primaryIndex.SeekPrefix(map[string]any{
		t.db.PrimaryKey(command.Collection): command.DocID,
	}).SetDocumentID(command.DocID).Path(), command.After.Bytes()); err != nil {
		return errors.Wrap(err, errors.Internal, "failed to batch set documents to primary index")
	}
	return nil
}

func (t *transaction) persistCommand(ctx context.Context, md *model.Metadata, command *model.Command) error {
	if md.Exists(string(isIndexingKey)) {
		for _, i := range t.db.collections.Get(command.Collection).indexing {
			if i.Primary {
				continue
			}
			if err := t.updateSecondaryIndex(ctx, i, command); err != nil {
				return errors.Wrap(err, errors.Internal, "")
			}
		}
		return nil
	}
	if err := t.db.applyPersistHooks(ctx, t, command, true); err != nil {
		return err
	}
	c := t.db.collections.Get(command.Collection)
	if c == nil {
		return fmt.Errorf("collection: %s does not exist", command.Collection)
	}
	if command.Timestamp.IsZero() {
		command.Timestamp = time.Now()
	}
	if command.Metadata == nil {
		md, _ := model.GetMetadata(ctx)
		command.Metadata = md
	}
	before, _ := t.db.Get(ctx, command.Collection, command.DocID)
	if before == nil || !before.Valid() {
		before = model.NewDocument()
	}
	command.Before = before
	switch command.Action {
	case model.Update:
		if err := t.updateDocument(ctx, c, command); err != nil {
			return err
		}
	case model.Create:
		if err := t.createDocument(ctx, c, command); err != nil {
			return err
		}
	case model.Delete:
		if err := t.deleteDocument(ctx, c, command); err != nil {
			return err
		}
	case model.Set:
		if err := t.setDocument(ctx, c, command); err != nil {
			return err
		}
	}
	for _, i := range t.db.collections.Get(command.Collection).indexing {
		if i.Primary {
			continue
		}
		if err := t.updateSecondaryIndex(ctx, i, command); err != nil {
			return errors.Wrap(err, errors.Internal, "")
		}
	}
	if err := t.db.applyPersistHooks(ctx, t, command, false); err != nil {
		return err
	}
	return nil
}

func (t *transaction) updateSecondaryIndex(ctx context.Context, idx model.Index, command *model.Command) error {
	if idx.Primary {
		return nil
	}
	switch command.Action {
	case model.Delete:
		if err := t.tx.Delete(idx.SeekPrefix(command.Before.Value()).SetDocumentID(command.DocID).Path()); err != nil {
			return errors.Wrap(
				err,
				errors.Internal,
				"failed to delete document %s/%s index references",
				command.Collection,
				command.DocID,
			)
		}
	case model.Set, model.Update, model.Create:
		if command.Before != nil {
			if err := t.tx.Delete(idx.SeekPrefix(command.Before.Value()).SetDocumentID(command.DocID).Path()); err != nil {
				return errors.Wrap(
					err,
					errors.Internal,
					"failed to delete document %s/%s index references",
					command.Collection,
					command.DocID,
				)
			}
		}
		if idx.Unique && !idx.Primary && command.After != nil {
			if err := t.db.kv.Tx(false, func(tx kv.Tx) error {
				it := tx.NewIterator(kv.IterOpts{
					Prefix: idx.SeekPrefix(command.After.Value()).Path(),
				})
				defer it.Close()
				for it.Valid() {
					item := it.Item()
					split := bytes.Split(item.Key(), []byte("\x00"))
					id := split[len(split)-1]
					if string(id) != command.DocID {
						return errors.New(errors.Internal, "duplicate value( %s ) found for unique index: %s", command.DocID, idx.Name)
					}
					it.Next()
				}
				return nil
			}); err != nil {
				return errors.Wrap(
					err,
					errors.Internal,
					"failed to set document %s/%s index references",
					command.Collection,
					command.DocID,
				)
			}
		}
		// only persist ids in secondary index - lookup full document in primary index
		if err := t.tx.Set(idx.SeekPrefix(command.After.Value()).SetDocumentID(command.DocID).Path(), []byte(command.DocID)); err != nil {
			return errors.Wrap(
				err,
				errors.Internal,
				"failed to set document %s/%s index references",
				command.Collection,
				command.DocID,
			)
		}
	}
	return nil
}
