package tikv

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/autom8ter/gokvkit/kv"
	"github.com/stretchr/testify/assert"
)

func Test(t *testing.T) {
	db, err := open("")
	assert.NoError(t, err)
	data := map[string]string{}
	for i := 0; i < 10; i++ {
		data[fmt.Sprint(i)] = fmt.Sprint(i)
	}
	t.Run("set", func(t *testing.T) {
		assert.Nil(t, db.Tx(false, func(tx kv.Tx) error {
			for k, v := range data {
				assert.Nil(t, tx.Set(context.Background(), []byte(k), []byte(v)))
			}
			return nil
		}))
	})

	t.Run("get", func(t *testing.T) {
		assert.Nil(t, db.Tx(true, func(tx kv.Tx) error {
			for k, v := range data {
				data, err := tx.Get(context.Background(), []byte(k))
				assert.NoError(t, err)
				assert.EqualValues(t, string(v), string(data))
			}
			return nil
		}))
	})
	t.Run("iterate", func(t *testing.T) {
		assert.Nil(t, db.Tx(true, func(tx kv.Tx) error {
			iter, err := tx.NewIterator(kv.IterOpts{
				Prefix:  nil,
				Seek:    nil,
				Reverse: false,
			})
			assert.NoError(t, err)
			defer iter.Close()
			i := 0
			for iter.Valid() {
				i++
				val, _ := iter.Value()
				assert.EqualValues(t, string(val), data[string(iter.Key())])
				iter.Next()
			}
			assert.Equal(t, len(data), i)
			return nil
		}))
	})
	t.Run("delete", func(t *testing.T) {
		assert.Nil(t, db.Tx(false, func(tx kv.Tx) error {
			for k, _ := range data {
				assert.Nil(t, tx.Delete(context.Background(), []byte(k)))
			}
			for k, _ := range data {
				bytes, _ := tx.Get(context.Background(), []byte(k))
				assert.Nil(t, bytes)
			}
			return nil
		}))
	})
	t.Run("locker", func(t *testing.T) {
		lock, err := db.NewLocker([]byte("testing"), 1*time.Second)
		assert.NoError(t, err)
		{
			gotLock, err := lock.TryLock(context.Background())
			assert.NoError(t, err)
			assert.True(t, gotLock)
			is, err := lock.IsLocked(context.Background())
			assert.NoError(t, err)
			assert.True(t, is)
		}
		{
			gotLock, err := lock.TryLock(context.Background())
			assert.NoError(t, err)
			assert.False(t, gotLock)
		}
		{
			lock.Unlock()
			assert.NoError(t, err)
		}

		newLock, err := db.NewLocker([]byte("testing"), 1*time.Second)
		assert.NoError(t, err)
		gotLock, err := newLock.TryLock(context.Background())
		assert.NoError(t, err)
		assert.True(t, gotLock)

		gotLock, err = lock.TryLock(context.Background())
		assert.NoError(t, err)
		assert.False(t, gotLock)
	})
	t.Run("set", func(t *testing.T) {
		assert.Nil(t, db.Tx(false, func(tx kv.Tx) error {
			for k, v := range data {
				assert.Nil(t, tx.Set(context.Background(), []byte(k), []byte(v)))
			}
			for k, _ := range data {
				_, err := tx.Get(context.Background(), []byte(k))
				assert.NoError(t, err)
			}
			return nil
		}))
	})
	t.Run("new tx", func(t *testing.T) {
		tx, err := db.NewTx(false)
		assert.NoError(t, err)
		defer func() {
			assert.NoError(t, tx.Commit(context.Background()))
		}()
		for k, v := range data {
			assert.Nil(t, tx.Set(context.Background(), []byte(k), []byte(v)))
		}
		for k, _ := range data {
			_, err := tx.Get(context.Background(), []byte(k))
			assert.NoError(t, err)
		}
	})
	t.Run("new tx w/ rollback", func(t *testing.T) {
		tx, err := db.NewTx(false)
		assert.NoError(t, err)
		for k, v := range data {
			assert.Nil(t, tx.Set(context.Background(), []byte(k), []byte(v)))
		}
		tx.Rollback(context.Background())
		for k, _ := range data {
			val, _ := tx.Get(context.Background(), []byte(k))
			assert.Empty(t, val)
		}
	})
	t.Run("drop prefix", func(t *testing.T) {
		{
			tx, err := db.NewTx(false)
			assert.NoError(t, err)
			for k, v := range data {
				assert.Nil(t, tx.Set(context.Background(), []byte(fmt.Sprintf("testing.%s", k)), []byte(v)))
			}
			assert.NoError(t, tx.Commit(context.Background()))
		}
		assert.NoError(t, db.DropPrefix(context.Background(), []byte("testing.")))
		count := 0
		assert.NoError(t, db.Tx(true, func(tx kv.Tx) error {
			iter, err := tx.NewIterator(kv.IterOpts{Prefix: []byte("testing.")})
			assert.NoError(t, err)
			defer iter.Close()
			for iter.Valid() {
				_, err = iter.Value()
				assert.NoError(t, err)
				count++
				iter.Next()
			}
			return nil
		}))
		assert.Equal(t, 0, count)
	})

}
