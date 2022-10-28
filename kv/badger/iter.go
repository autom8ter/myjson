package badger

import (
	"github.com/autom8ter/wolverine/kv"
	"github.com/dgraph-io/badger/v3"
)

type badgerIterator struct {
	opts kv.IterOpts
	iter *badger.Iterator
}

func (b *badgerIterator) Seek(key []byte) {
	b.iter.Seek(key)
}

func (b *badgerIterator) Close() {
	b.iter.Close()
}

func (b *badgerIterator) Valid() bool {
	if b.opts.Prefix != nil {
		return b.iter.ValidForPrefix(b.opts.Prefix)
	}
	return b.iter.Valid()
}

func (b *badgerIterator) Item() kv.Item {
	return &item{item: b.iter.Item()}
}

func (b *badgerIterator) Next() {
	b.Next()
}
