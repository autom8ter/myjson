package badger

import (
	"bytes"

	"github.com/autom8ter/gokvkit/kv"
	"github.com/autom8ter/gokvkit/kv/kvutil"
	"github.com/dgraph-io/badger/v3"
)

type badgerTx struct {
	txn *badger.Txn
	db  *badgerKV
}

func (b *badgerTx) NewIterator(kopts kv.IterOpts) kv.Iterator {
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = true
	opts.PrefetchSize = 10
	opts.Prefix = kopts.Prefix
	opts.Reverse = kopts.Reverse
	if opts.Reverse && opts.Prefix != nil {
		opts.Prefix = kvutil.NextPrefix(kopts.Prefix)
	}
	iter := b.txn.NewIterator(opts)
	iter.Rewind()
	return &badgerIterator{iter: iter, opts: kopts}
}

func (b *badgerTx) Get(key []byte) ([]byte, error) {
	if bytes.HasPrefix(key, []byte("internal.")) {
		val, ok := b.db.cache.Get(key)
		if ok {
			return val.([]byte), nil
		}
	}
	i, err := b.txn.Get(key)
	if err != nil {
		return nil, err
	}
	val, err := i.ValueCopy(nil)
	return val, err
}

func (b *badgerTx) Set(key, value []byte) error {
	if err := b.txn.Set(key, value); err != nil {
		return err
	}
	if bytes.HasPrefix(key, []byte("internal.")) {
		b.db.cache.Set(key, value, 1)
	}
	return nil
}

func (b *badgerTx) Delete(key []byte) error {
	if bytes.HasPrefix(key, []byte("internal.")) {
		b.db.cache.Del(key)
	}
	return b.txn.Delete(key)
}

func (b *badgerTx) Rollback() {
	b.txn.Discard()
}

func (b *badgerTx) Commit() error {
	return b.txn.Commit()
}

func (b *badgerTx) Close() {
	b.txn.Discard()
}
