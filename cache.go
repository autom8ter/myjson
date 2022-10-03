package wolverine

import (
	"time"

	"github.com/dgraph-io/badger/v3"

	"github.com/autom8ter/wolverine/internal/prefix"
)

func (d *db) SetCache(key string, value string, expiration time.Time) error {
	return d.wrapErr(d.kv.Update(func(txn *badger.Txn) error {
		return txn.SetEntry(&badger.Entry{
			Key:       prefix.Cache(key),
			Value:     []byte(value),
			ExpiresAt: uint64(expiration.Unix()),
		})
	}), "")
}

func (d *db) DelCache(key string) error {
	return d.wrapErr(d.kv.Update(func(txn *badger.Txn) error {
		return txn.Delete(prefix.Cache(key))
	}), "")
}

func (d *db) GetCache(key string) (string, error) {
	var value string
	if err := d.kv.View(func(txn *badger.Txn) error {
		item, err := txn.Get(prefix.Cache(key))
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			value = string(val)
			return nil
		})
	}); err != nil {
		return "", err
	}
	return value, nil
}
