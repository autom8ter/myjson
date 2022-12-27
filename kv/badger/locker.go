package badger

import (
	"encoding/json"
	"time"

	"github.com/autom8ter/gokvkit/kv"
	"github.com/dgraph-io/badger/v3"
)

type badgerLock struct {
	id            string
	key           []byte
	db            *badgerKV
	leaseInterval time.Duration
	start         time.Time
	hasUnlocked   chan struct{}
	unlock        chan struct{}
}

type lockMeta struct {
	ID         string    `json:"id"`
	Start      time.Time `json:"start"`
	LastUpdate time.Time `json:"lastUpdate"`
	Key        []byte    `json:"key"`
}

func (b *badgerLock) IsLocked() (bool, error) {
	isLocked := true
	err := b.db.Tx(true, func(tx kv.Tx) error {
		val, err := tx.Get(b.key)
		if err != nil {
			if err != badger.ErrKeyNotFound {
				return err
			}
			isLocked = false
			return nil
		}
		var current lockMeta
		json.Unmarshal(val, &current)
		if time.Since(current.LastUpdate) > 4*b.leaseInterval && current.ID != b.id {
			isLocked = false
			return nil
		}
		return nil
	})
	return isLocked, err
}

func (b *badgerLock) TryLock() (bool, error) {
	b.start = time.Now()
	gotLock := false
	err := b.db.Tx(true, func(tx kv.Tx) error {
		val, err := tx.Get(b.key)
		if err != nil {
			if err != badger.ErrKeyNotFound {
				return err
			}
			if err := b.setLock(tx); err != nil {
				return err
			}
			gotLock = true
			return nil
		}
		var current lockMeta
		json.Unmarshal(val, &current)
		if time.Since(current.LastUpdate) > 4*b.leaseInterval && current.ID != b.id {
			if err := b.setLock(tx); err != nil {
				return err
			}
			gotLock = true
			return nil
		}
		return nil
	})
	if err == nil && gotLock {
		go b.keepalive()
	}
	return gotLock, err
}

func (b *badgerLock) Unlock() {
	b.unlock <- struct{}{}
	<-b.hasUnlocked
}

func (b *badgerLock) setLock(tx kv.Tx) error {
	meta := &lockMeta{
		ID:         b.id,
		Start:      b.start,
		LastUpdate: time.Now(),
		Key:        b.key,
	}
	bytes, _ := json.Marshal(meta)
	if err := tx.Set(
		b.key,
		bytes,

		0,
	); err != nil {
		return err
	}
	return nil
}

func (b *badgerLock) delLock(tx kv.Tx) error {
	return tx.Delete(b.key)
}

func (b *badgerLock) getLock(tx kv.Tx) (*lockMeta, error) {
	val, err := tx.Get(b.key)
	if err != nil {
		return nil, err
	}
	var m lockMeta
	if err := json.Unmarshal(val, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

func (b *badgerLock) keepalive() error {
	ticker := time.NewTicker(b.leaseInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			// update lease
			err := b.db.Tx(true, func(tx kv.Tx) error {
				val, err := b.getLock(tx)
				if err != nil {
					return err
				}
				if val.ID == b.id {
					return b.setLock(tx)
				}
				return nil
			})
			if err != nil {
				return err
			}
		case <-b.unlock:
			err := b.db.Tx(true, func(tx kv.Tx) error {
				val, err := b.getLock(tx)
				if err != nil {
					return err
				}
				if val.ID == b.id {
					return b.delLock(tx)
				}
				return nil
			})
			b.hasUnlocked <- struct{}{}
			return err
		}
	}
}
