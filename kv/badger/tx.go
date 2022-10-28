package badger

import (
	"github.com/autom8ter/wolverine/kv"
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
	return &badgerIterator{iter: b.txn.NewIterator(opts), opts: kopts}
}

func (b badgerTx) Get(key []byte) ([]byte, error) {
	item, err := b.txn.Get(key)
	if err != nil {
		return nil, err
	}
	val, err := item.ValueCopy(nil)
	return val, err
}

func (b badgerTx) Set(key, value []byte) error {
	return b.txn.Set(key, value)
}

func (b badgerTx) Delete(key []byte) error {
	return b.txn.Delete(key)
}
