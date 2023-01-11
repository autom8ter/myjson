package badger

import (
	"bytes"
	"context"
	"time"

	"github.com/autom8ter/machine/v4"
	"github.com/autom8ter/myjson/kv"
	"github.com/autom8ter/myjson/kv/registry"
	"github.com/dgraph-io/badger/v3"
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
	return &badgerKV{
		db:      db,
		machine: machine.New(),
	}, nil
}

func (b *badgerKV) Tx(opts kv.TxOpts, fn func(kv.Tx) error) error {
	tx, err := b.NewTx(opts)
	if err != nil {
		return err
	}
	err = fn(tx)
	if err != nil {
		//nolint:errcheck
		tx.Rollback(context.Background())
		return err
	}
	if err := tx.Commit(context.Background()); err != nil {
		return err
	}
	return nil
}

func (b *badgerKV) NewTx(opts kv.TxOpts) (kv.Tx, error) {
	if opts.IsBatch {
		return &badgerTx{
			opts:    opts,
			batch:   b.db.NewWriteBatch(),
			db:      b,
			machine: b.machine,
		}, nil
	}
	return &badgerTx{
		opts:    opts,
		txn:     b.db.NewTransaction(!opts.IsReadOnly),
		db:      b,
		machine: b.machine,
	}, nil
}

func (b *badgerKV) Close(ctx context.Context) error {
	if err := b.db.Sync(); err != nil {
		return err
	}
	return b.db.Close()
}

func (b *badgerKV) DropPrefix(ctx context.Context, prefix ...[]byte) error {
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

//
//func (b *badgerKV) easyGet(ctx context.Context, key []byte) ([]byte, error) {
//	var (
//		val []byte
//		err error
//	)
//	err = b.Tx(kv.TxOpts{IsReadOnly: true}, func(tx kv.Tx) error {
//		val, err = tx.Get(ctx, key)
//		return err
//	})
//	return val, err
//}
