package tikv

import (
	"context"

	"github.com/autom8ter/gokvkit/kv"
	tikvErr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/txnkv/transaction"
)

type tikvTx struct {
	txn      *transaction.KVTxn
	readOnly bool
	db       *tikvKV
}

func (t *tikvTx) NewIterator(kopts kv.IterOpts) (kv.Iterator, error) {
	iter, err := t.txn.Iter(kopts.Prefix, nil)
	if err != nil {
		return nil, err
	}

	// iter.Seek(kopts.Seek) // TODO: how to seek?
	return &tikvIterator{iter: iter, opts: kopts}, nil
}

func (t *tikvTx) Get(ctx context.Context, key []byte) ([]byte, error) {
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
	if err := t.txn.Set(key, value); err != nil {
		return err
	}
	return nil
}

func (t *tikvTx) Delete(ctx context.Context, key []byte) error {
	return t.txn.Delete(key)
}

func (t *tikvTx) Rollback(ctx context.Context) {
	t.txn.Rollback()
}

func (t *tikvTx) Commit(ctx context.Context) error {
	return t.txn.Commit(ctx)
}

func (t *tikvTx) Close(ctx context.Context) {
	// TODO: how to close?
	return
}
