package badger

import (
	"bytes"

	"github.com/autom8ter/gokvkit/kv"
	"github.com/autom8ter/gokvkit/kv/registry"
	"github.com/dgraph-io/badger/v3"
	"github.com/dgraph-io/ristretto"
	"github.com/spf13/cast"
)

func init() {
	registry.Register("badger", func(params map[string]interface{}) (kv.DB, error) {
		return open(cast.ToString(params["storage_path"]))
	})
}

type badgerKV struct {
	db    *badger.DB
	cache *ristretto.Cache
}

func open(storagePath string) (kv.DB, error) {
	opts := badger.DefaultOptions(storagePath)
	if storagePath == "" {
		opts.InMemory = true
		opts.Dir = ""
		opts.ValueDir = ""
	}
	opts = opts.WithLoggingLevel(badger.ERROR)
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	cache, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: 10000, // number of keys to track frequency of (10M).
		MaxCost:     1000,  // maximum cost of cache (1GB).
		BufferItems: 64,    // number of keys per Get buffer.
	})
	if err != nil {
		return nil, err
	}
	return &badgerKV{
		db:    db,
		cache: cache,
	}, nil
}

func (b *badgerKV) Tx(isUpdate bool, fn func(kv.Tx) error) error {
	if isUpdate {
		return b.db.Update(func(txn *badger.Txn) error {
			return fn(&badgerTx{txn: txn, db: b})
		})
	}
	return b.db.View(func(txn *badger.Txn) error {
		return fn(&badgerTx{txn: txn, db: b})
	})
}

func (b *badgerKV) NewTx(isUpdate bool) kv.Tx {
	return &badgerTx{txn: b.db.NewTransaction(isUpdate), db: b}
}

func (b *badgerKV) NewBatch() kv.Batch {
	return &badgerBatch{batch: b.db.NewWriteBatch()}
}

func (b *badgerKV) Close() error {
	if err := b.db.Sync(); err != nil {
		return err
	}
	b.cache.Close()
	return b.db.Close()
}

func (b *badgerKV) DropPrefix(prefix ...[]byte) error {
	for _, p := range prefix {
		if bytes.HasPrefix(p, []byte("internal.")) {
			b.cache.Clear()
			break
		}
	}
	return b.db.DropPrefix(prefix...)
}
