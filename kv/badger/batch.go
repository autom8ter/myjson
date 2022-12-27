package badger

import (
	"time"

	"github.com/dgraph-io/badger/v3"
)

type badgerBatch struct {
	batch *badger.WriteBatch
}

func (b *badgerBatch) Set(key, value []byte, ttl time.Duration) error {
	var e = &badger.Entry{
		Key:   key,
		Value: value,
	}
	if ttl != 0 {
		e.ExpiresAt = uint64(time.Now().Add(ttl).Unix())
	}
	return b.batch.SetEntry(e)
}

func (b *badgerBatch) Delete(key []byte) error {
	return b.batch.Delete(key)
}

func (b *badgerBatch) Flush() error {
	return b.batch.Flush()
}
