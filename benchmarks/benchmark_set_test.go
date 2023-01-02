package benchmarks

import (
	"context"
	"testing"

	"github.com/autom8ter/gokvkit"
	"github.com/autom8ter/gokvkit/testutil"
	"github.com/stretchr/testify/assert"
)

// BenchmarkSet-12    	     967	   1255234 ns/op	  620238 B/op	    7615 allocs/op
func BenchmarkSet(b *testing.B) {
	doc := testutil.NewUserDoc()
	b.ReportAllocs()
	assert.Nil(b, testutil.TestDB(func(ctx context.Context, db gokvkit.Database) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			assert.Nil(b, db.Tx(ctx, gokvkit.TxOpts{IsReadOnly: false}, func(ctx context.Context, tx gokvkit.Tx) error {
				return tx.Set(ctx, "user", doc)
			}))
		}
	}))
}

// BenchmarkSet1000-12    	       1	2627088366 ns/op	458302760 B/op	 4815781 allocs/op
func BenchmarkSet1000(b *testing.B) {
	doc := testutil.NewUserDoc()
	b.ReportAllocs()
	assert.Nil(b, testutil.TestDB(func(ctx context.Context, db gokvkit.Database) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			assert.Nil(b, db.Tx(ctx, gokvkit.TxOpts{IsReadOnly: false}, func(ctx context.Context, tx gokvkit.Tx) error {
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
