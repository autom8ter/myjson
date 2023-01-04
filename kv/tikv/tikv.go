package tikv

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/autom8ter/myjson/kv"
	"github.com/autom8ter/myjson/kv/registry"
	"github.com/go-redis/redis/v9"
	"github.com/segmentio/ksuid"
	"github.com/spf13/cast"
	"github.com/tikv/client-go/v2/txnkv"
)

func init() {
	registry.Register("tikv", func(params map[string]interface{}) (kv.DB, error) {
		if params["pd_addr"] == nil {
			return nil, fmt.Errorf("'pd_addr' is a required paramater")
		}
		if params["redis_addr"] == nil {
			return nil, fmt.Errorf("'redis_addr' is a required paramater")
		}
		return open(params)
	})
}

type tikvKV struct {
	db    *txnkv.Client
	cache *redis.Client
}

func open(params map[string]interface{}) (kv.DB, error) {
	pdAddr := cast.ToStringSlice(params["pd_addr"])
	if len(pdAddr) == 0 {
		return nil, fmt.Errorf("empty pd address")
	}
	client, err := txnkv.NewClient(pdAddr)
	if err != nil {
		return nil, err
	}
	cache := redis.NewClient(&redis.Options{
		Addr:     cast.ToString(params["redis_addr"]),
		Username: cast.ToString(params["redis_user"]),
		Password: cast.ToString(params["redis_password"]),
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cache.Ping(ctx); err != nil && err.Err() != nil {
		return nil, fmt.Errorf("failed to ping redis instance(%s): %s", cast.ToString(params["redis_addr"]), err.Err())
	}
	return &tikvKV{
		db:    client,
		cache: cache,
	}, nil
}

func (b *tikvKV) Tx(opts kv.TxOpts, fn func(kv.Tx) error) error {
	tx, err := b.NewTx(opts)
	if err != nil {
		return err
	}
	err = fn(tx)
	if err != nil {
		if rollbackErr := tx.Rollback(context.Background()); rollbackErr != nil {
			return fmt.Errorf("%s - failed to rollback transaction: %s", err.Error(), rollbackErr.Error())
		}
		return err
	}
	if err := tx.Commit(context.Background()); err != nil {
		return err
	}
	return nil
}

func (b *tikvKV) NewTx(opts kv.TxOpts) (kv.Tx, error) {
	tx, err := b.db.Begin()
	if err != nil {
		return nil, err
	}
	if !tx.Valid() {
		return nil, fmt.Errorf("invalid transaction")
	}
	return &tikvTx{txn: tx, db: b, opts: opts}, nil
}

func (b *tikvKV) Close(ctx context.Context) error {
	return b.db.Close()
}

func (b *tikvKV) DropPrefix(ctx context.Context, prefix ...[]byte) error {
	for _, p := range prefix {
		if _, err := b.db.DeleteRange(ctx, p, nil, 1); err != nil {
			return err
		}
	}
	return nil
}

func (b *tikvKV) NewLocker(key []byte, leaseInterval time.Duration) (kv.Locker, error) {
	return &tikvLock{
		id:            ksuid.New().String(),
		key:           key,
		db:            b,
		leaseInterval: leaseInterval,
		unlock:        make(chan struct{}),
		hasUnlocked:   make(chan struct{}),
	}, nil
}

func (b *tikvKV) ChangeStream(ctx context.Context, prefix []byte, fn kv.ChangeStreamHandler) error {
	ch := b.cache.PSubscribe(ctx, "*").Channel()
	for {
		select {
		case <-ctx.Done():
			return nil
		case msg := <-ch:
			var cdc kv.CDC
			//nolint:errcheck
			json.Unmarshal([]byte(msg.Payload), &cdc)
			if bytes.HasPrefix(cdc.Key, prefix) {
				contn, err := fn(cdc)
				if err != nil {
					return err
				}
				if !contn {
					return nil
				}
			}
		}
	}
}
