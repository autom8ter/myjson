package badger

import (
	"github.com/autom8ter/brutus/kv"
	"github.com/autom8ter/brutus/kv/kvutil"
	"github.com/dgraph-io/badger/v3"
)

type badgerTx struct {
	txn *badger.Txn
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
	i, err := b.txn.Get(key)
	if err != nil {
		return nil, err
	}
	val, err := i.ValueCopy(nil)
	return val, err
}

func (b *badgerTx) Set(key, value []byte) error {
	return b.txn.Set(key, value)
}

func (b *badgerTx) Delete(key []byte) error {
	return b.txn.Delete(key)
}
