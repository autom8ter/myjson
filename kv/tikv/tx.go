package tikv

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/autom8ter/gokvkit/kv"
	"github.com/autom8ter/gokvkit/kv/kvutil"
	tikvErr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/txnkv/transaction"
)

type tikvTx struct {
	txn     *transaction.KVTxn
	opts    kv.TxOpts
	db      *tikvKV
	entries []kv.CDC
}

func (t *tikvTx) NewIterator(kopts kv.IterOpts) (kv.Iterator, error) {
	if kopts.Reverse {
		if kopts.Seek == nil {
			iter, err := t.txn.IterReverse(kvutil.NextPrefix(kopts.UpperBound))
			if err != nil {
				return nil, err
			}
			// iter.Seek(kopts.Seek) // TODO: how to seek?
			return &tikvIterator{iter: iter, opts: kopts}, nil
		} else {
			iter, err := t.txn.IterReverse(kvutil.NextPrefix(kopts.Seek))
			if err != nil {
				return nil, err
			}
			return &tikvIterator{iter: iter, opts: kopts}, nil
		}

	}
	iter, err := t.txn.Iter(kopts.Prefix, kvutil.NextPrefix(kopts.UpperBound))
	if err != nil {
		return nil, err
	}
	// iter.Seek(kopts.Seek) // TODO: how to seek?
	return &tikvIterator{iter: iter, opts: kopts}, nil
}

func (t *tikvTx) Get(ctx context.Context, key []byte) ([]byte, error) {
	{
		val, _ := t.db.cache.Get(ctx, string(key)).Result()
		if val != "" {
			return []byte(val), nil
		}
	}

	val, err := t.txn.Get(ctx, key)
	if err != nil {
		if tikvErr.IsErrNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return val, err
}

func (t *tikvTx) Set(ctx context.Context, key, value []byte) error {
	if t.opts.IsReadOnly {
		return fmt.Errorf("writes forbidden in read-only transaction")
	}
	if err := t.txn.Set(key, value); err != nil {
		return err
	}
	t.entries = append(t.entries, kv.CDC{
		Operation: kv.SETOP,
		Key:       key,
		Value:     value,
	})
	return nil
}

func (t *tikvTx) Delete(ctx context.Context, key []byte) error {
	if t.opts.IsReadOnly {
		return fmt.Errorf("writes forbidden in read-only transaction")
	}
	if err := t.txn.Delete(key); err != nil {
		return err
	}
	t.db.cache.Del(ctx, string(key))
	t.entries = append(t.entries, kv.CDC{
		Operation: kv.DELOP,
		Key:       key,
	})
	return nil
}

func (t *tikvTx) Rollback(ctx context.Context) {
	t.txn.Rollback()
	t.entries = []kv.CDC{}
}

func (t *tikvTx) Commit(ctx context.Context) error {
	if err := t.txn.Commit(ctx); err != nil {
		return err
	}
	for _, e := range t.entries {
		bits, err := json.Marshal(e)
		if err != nil {
			return err
		}
		t.db.cache.Publish(ctx, string(e.Key), bits)
		switch e.Operation {
		case kv.DELOP:
			t.db.cache.Del(ctx, string(e.Key))
		case kv.SETOP:
			t.db.cache.Set(ctx, string(e.Key), string(e.Value), 1*time.Hour)
		}
	}
	t.entries = []kv.CDC{}
	return nil
}

func (t *tikvTx) Close(ctx context.Context) {
	t.entries = []kv.CDC{}
	return
}
