package badger

import (
	"fmt"
	"testing"
	"time"

	"github.com/autom8ter/gokvkit/kv"
	"github.com/stretchr/testify/assert"
)

func Test(t *testing.T) {
	db, err := open("")
	assert.Nil(t, err)
	data := map[string]string{}
	for i := 0; i < 10; i++ {
		data[fmt.Sprint(i)] = fmt.Sprint(i)
	}
	t.Run("set", func(t *testing.T) {
		assert.Nil(t, db.Tx(true, func(tx kv.Tx) error {
			for k, v := range data {
				assert.Nil(t, tx.Set([]byte(k), []byte(v)))
			}
			return nil
		}))
	})

	t.Run("get", func(t *testing.T) {
		assert.Nil(t, db.Tx(false, func(tx kv.Tx) error {
			for k, v := range data {
				data, err := tx.Get([]byte(k))
				assert.Nil(t, err)
				assert.EqualValues(t, string(v), string(data))
			}
			return nil
		}))
	})
	t.Run("batch", func(t *testing.T) {
		batch := db.NewBatch()
		for k, v := range data {
			assert.Nil(t, batch.Set([]byte(k), []byte(v)))
		}
		assert.Nil(t, batch.Flush())
		assert.Nil(t, db.Tx(false, func(tx kv.Tx) error {
			for k, v := range data {
				data, err := tx.Get([]byte(k))
				assert.Nil(t, err)
				assert.EqualValues(t, string(v), string(data))
			}
			return nil
		}))
	})
	t.Run("iterate", func(t *testing.T) {
		assert.Nil(t, db.Tx(false, func(tx kv.Tx) error {
			iter := tx.NewIterator(kv.IterOpts{
				Prefix:  nil,
				Seek:    nil,
				Reverse: false,
			})
			defer iter.Close()
			i := 0
			for iter.Valid() {
				i++
				item := iter.Item()
				val, _ := item.Value()
				assert.EqualValues(t, string(val), data[string(item.Key())])
				iter.Next()
			}
			assert.Equal(t, len(data), i)
			return nil
		}))
	})
	t.Run("delete", func(t *testing.T) {
		assert.Nil(t, db.Tx(true, func(tx kv.Tx) error {
			for k, _ := range data {
				assert.Nil(t, tx.Delete([]byte(k)))
			}
			for k, _ := range data {
				_, err := tx.Get([]byte(k))
				assert.NotNil(t, err)
			}
			return nil
		}))
	})
	t.Run("batch set then delete", func(t *testing.T) {
		batch := db.NewBatch()
		for k, v := range data {
			assert.Nil(t, batch.Set([]byte(k), []byte(v)))
		}
		assert.Nil(t, batch.Flush())
		batch2 := db.NewBatch()
		for k, _ := range data {
			assert.Nil(t, batch2.Delete([]byte(k)))
		}
		assert.Nil(t, batch2.Flush())
		assert.Nil(t, db.Tx(false, func(tx kv.Tx) error {
			for k, _ := range data {
				_, err := tx.Get([]byte(k))
				assert.NotNil(t, err)
			}
			return nil
		}))
	})
	t.Run("locker", func(t *testing.T) {
		lock := db.NewLocker([]byte("testing"), 1*time.Second)
		{
			gotLock, err := lock.TryLock()
			assert.Nil(t, err)
			assert.True(t, gotLock)
		}
		{
			gotLock, err := lock.TryLock()
			assert.Nil(t, err)
			assert.False(t, gotLock)
		}
		{
			lock.Unlock()
			assert.Nil(t, err)
		}

		newLock := db.NewLocker([]byte("testing"), 1*time.Second)
		gotLock, err := newLock.TryLock()
		assert.Nil(t, err)
		assert.True(t, gotLock)

		gotLock, err = lock.TryLock()
		assert.Nil(t, err)
		assert.False(t, gotLock)
	})
}
