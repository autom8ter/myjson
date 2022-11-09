package badger

import (
	"context"
	"github.com/autom8ter/brutus/kv"
	"github.com/autom8ter/brutus/kv/registry"
	"github.com/dgraph-io/badger/v3"
	"github.com/dgraph-io/badger/v3/pb"
	"github.com/spf13/cast"
)

func init() {
	registry.Register("badger", func(params map[string]interface{}) (kv.DB, error) {
		return open(cast.ToString(params["storage_path"]))
	})
}

type badgerKV struct {
	db *badger.DB
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

func (b *badgerKV) Stream(ctx context.Context, prefix []byte, handler kv.StreamHandler) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	err := b.db.Subscribe(ctx, func(list *badger.KVList) error {
		var items []kv.Item
		for _, l := range list.Kv {
			items = append(items, &item{
				key: l.Key,
				value: func() ([]byte, error) {
					return l.Value, nil
				},
			})
		}
		continu, err := handler(ctx, items)
		if err != nil {
			return err
		}
		if !continu {
			cancel()
		}
		return nil
	}, []pb.Match{{
		Prefix: prefix,
	}})
	if err.Error() == "context canceled" {
		return nil
	}
	return err
}
