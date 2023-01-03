package benchmarks

import (
	"context"
	"testing"

	"github.com/autom8ter/myjson"
	"github.com/autom8ter/myjson/kv"
	"github.com/autom8ter/myjson/testutil"
	"github.com/stretchr/testify/assert"
)

// BenchmarkSet-12    	    1648	    651361 ns/op	  366783 B/op	    4600 allocs/op
func BenchmarkSet(b *testing.B) {
	doc := testutil.NewUserDoc()
	assert.Nil(b, testutil.TestDB(func(ctx context.Context, db myjson.Database) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			assert.Nil(b, db.Tx(ctx, kv.TxOpts{IsReadOnly: false}, func(ctx context.Context, tx myjson.Tx) error {
				return tx.Set(ctx, "user", doc)
			}))
		}
	}))
}

// BenchmarkSet1000-12    	       3	 348299819 ns/op	188733965 B/op	 2011669 allocs/op
func BenchmarkSet1000(b *testing.B) {
	doc := testutil.NewUserDoc()
	b.ReportAllocs()
	assert.Nil(b, testutil.TestDB(func(ctx context.Context, db myjson.Database) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			assert.Nil(b, db.Tx(ctx, kv.TxOpts{IsReadOnly: false, IsBatch: true}, func(ctx context.Context, tx myjson.Tx) error {
				for v := 0; v < 1000; v++ {
					if err := tx.Set(ctx, "user", doc); err != nil {
						return err
					}
				}
				return nil
			}))
		}
	}))
}
