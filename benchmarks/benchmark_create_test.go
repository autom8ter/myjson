package benchmarks

import (
	"context"
	"testing"

	"github.com/autom8ter/gokvkit"
	"github.com/autom8ter/gokvkit/kv"
	"github.com/autom8ter/gokvkit/testutil"
	"github.com/stretchr/testify/assert"
)

// BenchmarkCreate-12    	     954	   1237137 ns/op	  601310 B/op	    7459 allocs/op
func BenchmarkCreate(b *testing.B) {
	doc := testutil.NewUserDoc()
	b.ReportAllocs()
	assert.Nil(b, testutil.TestDB(func(ctx context.Context, db gokvkit.Database) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			assert.Nil(b, db.Tx(ctx, kv.TxOpts{IsReadOnly: false}, func(ctx context.Context, tx gokvkit.Tx) error {
				_, err := tx.Create(ctx, "user", doc)
				return err
			}))
		}
	}))
}

// BenchmarkCreate1000-12    	       1	2662527299 ns/op	403135712 B/op	 4319974 allocs/op
func BenchmarkCreate1000(b *testing.B) {
	doc := testutil.NewUserDoc()
	b.ReportAllocs()
	assert.Nil(b, testutil.TestDB(func(ctx context.Context, db gokvkit.Database) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			assert.Nil(b, db.Tx(ctx, kv.TxOpts{IsReadOnly: false}, func(ctx context.Context, tx gokvkit.Tx) error {
				for v := 0; v < 1000; v++ {
					if _, err := tx.Create(ctx, "user", doc); err != nil {
						return err
					}
				}
				return nil
			}))
		}
	}))
}
