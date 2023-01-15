package benchmarks

import (
	"context"
	"testing"

	"github.com/autom8ter/myjson"
	"github.com/autom8ter/myjson/kv"
	"github.com/autom8ter/myjson/testutil"
	"github.com/stretchr/testify/assert"
)

// BenchmarkSet-12    	    1347	    925214 ns/op	  500704 B/op	    5961 allocs/op
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

// BenchmarkSet100000-12    	       1	59297905703 ns/op	29676942072 B/op	345757621 allocs/op
func BenchmarkSet100000(b *testing.B) {
	doc := testutil.NewUserDoc()
	b.ReportAllocs()
	assert.Nil(b, testutil.TestDB(func(ctx context.Context, db myjson.Database) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			assert.Nil(b, db.Tx(ctx, kv.TxOpts{IsReadOnly: false, IsBatch: true}, func(ctx context.Context, tx myjson.Tx) error {
				for v := 0; v < 100000; v++ {
					if err := tx.Set(ctx, "user", doc); err != nil {
						return err
					}
				}
				return nil
			}))
		}
	}))
}
