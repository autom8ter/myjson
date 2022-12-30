package tikv

import (
	"context"
	"fmt"
	"time"

	"github.com/autom8ter/gokvkit/kv"
	"github.com/autom8ter/gokvkit/kv/registry"
	"github.com/segmentio/ksuid"
	"github.com/spf13/cast"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/txnkv"
)

func init() {
	registry.Register("tikv", func(params map[string]interface{}) (kv.DB, error) {
		if params["pd_addr"] == nil {
			return nil, fmt.Errorf("'pd_addr' is a required paramater")
		}
		return open(cast.ToString(params["pd_addr"]))
	})
}

type tikvKV struct {
	db *txnkv.Client
}

func open(pdAddr string) (kv.DB, error) {
	if pdAddr == "" {
		return nil, fmt.Errorf("empty pd address")
	}
	client, err := txnkv.NewClient([]string{pdAddr})
	if err != nil {
		return nil, err
	}
	return &tikvKV{
		db: client,
	}, nil
}

func (b *tikvKV) Tx(readOnly bool, fn func(kv.Tx) error) error {
	tx, err := b.NewTx(readOnly)
	if err != nil {
		return err
	}
	return fn(tx)
}

func (b *tikvKV) NewTx(readOnly bool) (kv.Tx, error) {
	if readOnly {
		tx, err := b.db.Begin(tikv.WithStartTS(uint64(time.Now().Unix()) + 2))
		if err != nil {
			return nil, err
		}
		return &tikvTx{txn: tx, db: b, readOnly: readOnly}, nil
	}
	tx, err := b.db.Begin()
	if err != nil {
		return nil, err
	}
	return &tikvTx{txn: tx, db: b, readOnly: readOnly}, nil
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
