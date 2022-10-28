package badger

import (
	"github.com/autom8ter/wolverine/kv"
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
	tx := b.db.NewTransaction(isUpdate)
	defer tx.Discard()
	if err := fn(&badgerTx{txn: tx}); err != nil {
		return err
	}
	return tx.Commit()
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
