package tikv

import (
	"bytes"

	"github.com/autom8ter/myjson/kv"
)

type unionStoreIterator interface {
	Valid() bool
	Key() []byte
	Value() []byte
	Next() error
	Close()
}

type tikvIterator struct {
	opts kv.IterOpts
	iter unionStoreIterator
}

func (b *tikvIterator) Seek(key []byte) {
	//b.iter.Seek(key)
	// TODO: how to seek?
}

func (b *tikvIterator) Close() {
	b.iter.Close()
}

func (b *tikvIterator) Valid() bool {
	if b.opts.Prefix != nil && !bytes.HasPrefix(b.Key(), b.opts.Prefix) {
		return false
	}
	if b.opts.UpperBound != nil && bytes.Compare(b.Key(), b.opts.UpperBound) == 1 {
		return false
	}
	return b.iter.Valid()
}

func (b *tikvIterator) Key() []byte {
	return b.iter.Key()
}

func (b *tikvIterator) Value() ([]byte, error) {
	return b.iter.Value(), nil
}

func (b *tikvIterator) Next() error {
	return b.iter.Next()
}
