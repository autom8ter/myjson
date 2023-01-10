package badger

import (
	"context"

	"github.com/autom8ter/machine/v4"
	"github.com/autom8ter/myjson/kv"
	"github.com/dgraph-io/badger/v3"
)

type badgerTx struct {
	opts    kv.TxOpts
	batch   *badger.WriteBatch
	txn     *badger.Txn
	db      *badgerKV
	machine machine.Machine
	entries []kv.CDC
}

func (b *badgerTx) NewIterator(kopts kv.IterOpts) (kv.Iterator, error) {
	if b.txn == nil {
		b.txn = b.db.db.NewTransaction(!b.opts.IsReadOnly)
	}
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = true
	opts.PrefetchSize = 10
	opts.Prefix = kopts.Prefix
	opts.Reverse = kopts.Reverse
	if kopts.Seek == nil && kopts.UpperBound != nil && kopts.Reverse {
		kopts.Seek = kopts.UpperBound
	}
	iter := b.txn.NewIterator(opts)
	if kopts.Seek == nil {
		iter.Rewind()
	}
	iter.Seek(kopts.Seek)
	return &badgerIterator{iter: iter, opts: kopts}, nil
}

func (b *badgerTx) Get(ctx context.Context, key []byte) ([]byte, error) {
	if b.txn == nil {
		b.txn = b.db.db.NewTransaction(!b.opts.IsReadOnly)
	}
	i, err := b.txn.Get(key)
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return nil, nil
		}
		return nil, err
	}
	val, err := i.ValueCopy(nil)
	return val, err
}

func (b *badgerTx) Set(ctx context.Context, key, value []byte) error {
	var e = &badger.Entry{
		Key:   key,
		Value: value,
	}
	if b.batch != nil {
		if err := b.batch.SetEntry(e); err != nil {
			return err
		}
	} else if b.txn != nil {
		if err := b.txn.SetEntry(e); err != nil {
			return err
		}
	}
	b.entries = append(b.entries, kv.CDC{
		Operation: kv.SETOP,
		Key:       key,
		Value:     value,
	})
	return nil
}

func (b *badgerTx) Delete(ctx context.Context, key []byte) error {
	b.entries = append(b.entries, kv.CDC{
		Operation: kv.DELOP,
		Key:       key,
	})
	if b.batch != nil {
		return b.batch.Delete(key)
	}
	return b.txn.Delete(key)
}

func (b *badgerTx) Rollback(ctx context.Context) error {
	if b.batch != nil {
		b.batch.Cancel()
	}
	if b.txn != nil {
		b.txn.Discard()
	}
	b.entries = []kv.CDC{}
	return nil
}

func (b *badgerTx) Commit(ctx context.Context) error {
	if b.batch != nil {
		if err := b.batch.Flush(); err != nil {
			return err
		}
	} else if b.txn != nil {
		if err := b.txn.Commit(); err != nil {
			return err
		}
	}
	for _, e := range b.entries {
		b.machine.Publish(ctx, machine.Message{
			Channel: string(e.Key),
			Body:    e,
		})
	}
	return nil
}

func (b *badgerTx) Close(ctx context.Context) {
	if b.txn != nil {
		b.txn.Discard()
	}
	if b.batch != nil {
		b.batch.Cancel()
	}
}
