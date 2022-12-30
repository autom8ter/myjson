package tikv

import (
	"context"
	"encoding/json"
	"time"

	"github.com/autom8ter/gokvkit/kv"
	tikvErr "github.com/tikv/client-go/v2/error"
)

type tikvLock struct {
	id            string
	key           []byte
	db            *tikvKV
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

func (b *tikvLock) IsLocked(ctx context.Context) (bool, error) {
	isLocked := true
	err := b.db.Tx(true, func(tx kv.Tx) error {
		val, err := tx.Get(ctx, b.key)
		if err != nil {
			if !tikvErr.IsErrNotFound(err) {
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

func (b *tikvLock) TryLock(ctx context.Context) (bool, error) {
	b.start = time.Now()
	gotLock := false
	err := b.db.Tx(false, func(tx kv.Tx) error {
		val, err := tx.Get(ctx, b.key)
		if err != nil {
			if !tikvErr.IsErrNotFound(err) {
				return err
			}
			if err := b.setLock(ctx, tx); err != nil {
				return err
			}
			gotLock = true
			return nil
		}
		var current lockMeta
		json.Unmarshal(val, &current)
		if time.Since(current.LastUpdate) > 4*b.leaseInterval && current.ID != b.id {
			if err := b.setLock(ctx, tx); err != nil {
				return err
			}
			gotLock = true
			return nil
		}
		return nil
	})
	if err == nil && gotLock {
		go b.keepalive(ctx)
	}
	return gotLock, err
}

func (b *tikvLock) Unlock() {
	b.unlock <- struct{}{}
	<-b.hasUnlocked
}

func (b *tikvLock) setLock(ctx context.Context, tx kv.Tx) error {
	meta := &lockMeta{
		ID:         b.id,
		Start:      b.start,
		LastUpdate: time.Now(),
		Key:        b.key,
	}
	bytes, _ := json.Marshal(meta)
	if err := tx.Set(
		ctx,
		b.key,
		bytes,
	); err != nil {
		return err
	}
	return nil
}

func (b *tikvLock) delLock(ctx context.Context, tx kv.Tx) error {
	return tx.Delete(ctx, b.key)
}

func (b *tikvLock) getLock(ctx context.Context, tx kv.Tx) (*lockMeta, error) {
	val, err := tx.Get(ctx, b.key)
	if err != nil {
		return nil, err
	}
	var m lockMeta
	if err := json.Unmarshal(val, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

func (b *tikvLock) keepalive(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	ticker := time.NewTicker(b.leaseInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			// update lease
			err := b.db.Tx(false, func(tx kv.Tx) error {
				val, err := b.getLock(ctx, tx)
				if err != nil {
					return err
				}
				if val.ID == b.id {
					return b.setLock(ctx, tx)
				}
				return nil
			})
			if err != nil {
				return err
			}
		case <-b.unlock:
			err := b.db.Tx(false, func(tx kv.Tx) error {
				val, err := b.getLock(ctx, tx)
				if err != nil {
					return err
				}
				if val.ID == b.id {
					return b.delLock(ctx, tx)
				}
				return nil
			})
			b.hasUnlocked <- struct{}{}
			return err
		}
	}
}
