package badger

import "github.com/dgraph-io/badger/v3"

type badgerBatch struct {
	batch *badger.WriteBatch
}

func (b *badgerBatch) Set(key, value []byte) error {
	return b.batch.Set(key, value)
}

func (b *badgerBatch) Delete(key []byte) error {
	return b.batch.Delete(key)
}

func (b *badgerBatch) Flush() error {
	return b.batch.Flush()
}
