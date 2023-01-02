package badger

import (
	"bytes"
	"context"
	"time"

	"github.com/autom8ter/gokvkit/kv"
	"github.com/autom8ter/gokvkit/kv/registry"
	"github.com/autom8ter/machine/v4"
	"github.com/dgraph-io/badger/v3"
	"github.com/dgraph-io/ristretto"
	"github.com/segmentio/ksuid"
	"github.com/spf13/cast"
)

func init() {
	registry.Register("badger", func(params map[string]interface{}) (kv.DB, error) {
		return open(cast.ToString(params["storage_path"]))
	})
}

type badgerKV struct {
	db      *badger.DB
	cache   *ristretto.Cache
	machine machine.Machine
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
		db:      db,
		cache:   cache,
		machine: machine.New(),
	}, nil
}

func (b *badgerKV) Tx(readOnly bool, fn func(kv.Tx) error) error {
	tx, err := b.NewTx(readOnly)
	if err != nil {
		return err
	}
	err = fn(tx)
	if err != nil {
		tx.Rollback(context.Background())
		return err
	}
	if err := tx.Commit(context.Background()); err != nil {
		return err
	}
	return nil
}

func (b *badgerKV) NewTx(readOnly bool) (kv.Tx, error) {
	return &badgerTx{txn: b.db.NewTransaction(!readOnly), db: b, machine: b.machine}, nil
}

func (b *badgerKV) Close(ctx context.Context) error {
	if err := b.db.Sync(); err != nil {
		return err
	}
	b.cache.Close()
	return b.db.Close()
}

func (b *badgerKV) DropPrefix(ctx context.Context, prefix ...[]byte) error {
	for _, p := range prefix {
		if bytes.HasPrefix(p, []byte("cache.")) {
			b.cache.Clear()
			break
		}
	}
	return b.db.DropPrefix(prefix...)
}

func (b *badgerKV) NewLocker(key []byte, leaseInterval time.Duration) (kv.Locker, error) {
	return &badgerLock{
		id:            ksuid.New().String(),
		key:           key,
		db:            b,
		leaseInterval: leaseInterval,
		unlock:        make(chan struct{}),
		hasUnlocked:   make(chan struct{}),
	}, nil
}

func (b *badgerKV) ChangeStream(ctx context.Context, prefix []byte, fn kv.ChangeStreamHandler) error {
	return b.machine.Subscribe(ctx, "*", func(ctx context.Context, msg machine.Message) (bool, error) {
		cdc := msg.Body.(kv.CDC)
		if bytes.HasPrefix(cdc.Key, prefix) {
			return fn(msg.Body.(kv.CDC))
		}
		return true, nil
	})
}
