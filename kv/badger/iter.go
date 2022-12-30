package badger

import (
	"bytes"

	"github.com/autom8ter/gokvkit/kv"
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
	if b.opts.Prefix != nil && !b.iter.ValidForPrefix(b.opts.Prefix) {
		return false
	}
	if b.opts.UpperBound != nil && bytes.Compare(b.Key(), b.opts.UpperBound) == 1 {
		return false
	}
	return b.iter.Valid()
}

func (b *badgerIterator) Key() []byte {
	return b.iter.Item().Key()
}

func (b *badgerIterator) Value() ([]byte, error) {
	return b.iter.Item().ValueCopy(nil)
}

func (b *badgerIterator) Next() error {
	b.iter.Next()
	return nil
}
