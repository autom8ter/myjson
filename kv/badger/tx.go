package badger

import (
	"bytes"
	"context"

	"github.com/autom8ter/gokvkit/kv"
	"github.com/dgraph-io/badger/v3"
)

type badgerTx struct {
	txn *badger.Txn
	db  *badgerKV
}

func (b *badgerTx) NewIterator(kopts kv.IterOpts) (kv.Iterator, error) {
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
	if bytes.HasPrefix(key, []byte("cache.")) {
		val, ok := b.db.cache.Get(key)
		if ok {
			return val.([]byte), nil
		}
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
	if err := b.txn.SetEntry(e); err != nil {
		return err
	}
	return nil
}

func (b *badgerTx) Delete(ctx context.Context, key []byte) error {
	if bytes.HasPrefix(key, []byte("cache.")) {
		b.db.cache.Del(key)
	}
	return b.txn.Delete(key)
}

func (b *badgerTx) Rollback(ctx context.Context) {
	b.txn.Discard()
}

func (b *badgerTx) Commit(ctx context.Context) error {
	return b.txn.Commit()
}

func (b *badgerTx) Close(ctx context.Context) {
	b.txn.Discard()
}
