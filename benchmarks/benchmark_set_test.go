package benchmarks

import (
	"context"
	"testing"

	"github.com/autom8ter/myjson"
	"github.com/autom8ter/myjson/kv"
	"github.com/autom8ter/myjson/testutil"
	"github.com/stretchr/testify/assert"
)

// BenchmarkSet-12    	    1118	   1081728 ns/op	  464895 B/op	    5772 allocs/op
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

// BenchmarkSet1000-12    	       2	 698652474 ns/op	243967400 B/op	 2734921 allocs/op
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
