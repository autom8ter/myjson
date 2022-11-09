package badger

import (
	"github.com/autom8ter/brutus/kv"
	"github.com/dgraph-io/badger/v3"
)

type badgerKV struct {
	db *badger.DB
}

func New(storagePath string) (kv.DB, error) {
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
	return &badgerKV{db: db}, nil
}

func (b *badgerKV) Tx(isUpdate bool, fn func(kv.Tx) error) error {
	if isUpdate {
		return b.db.Update(func(txn *badger.Txn) error {
			return fn(&badgerTx{txn: txn})
		})
	}
	return b.db.View(func(txn *badger.Txn) error {
		return fn(&badgerTx{txn: txn})
	})
}

func (b *badgerKV) Batch() kv.Batch {
	return &badgerBatch{batch: b.db.NewWriteBatch()}
}

func (b *badgerKV) Close() error {
	if err := b.db.Sync(); err != nil {
		return err
	}
	return b.db.Close()
}
