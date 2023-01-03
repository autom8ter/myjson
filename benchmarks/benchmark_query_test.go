package benchmarks

import (
	"context"
	"testing"

	"github.com/autom8ter/gokvkit"
	"github.com/autom8ter/gokvkit/testutil"
	"github.com/stretchr/testify/assert"
)

// BenchmarkQuery-12    	    3634	    290003 ns/op	  183447 B/op	    2365 allocs/op
func BenchmarkQuery(b *testing.B) {
	b.ReportAllocs()
	assert.Nil(b, testutil.TestDB(func(ctx context.Context, db gokvkit.Database) {
		assert.NoError(b, seedDatabase(ctx, db))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := db.Query(ctx, "account", gokvkit.Query{
				Where: []gokvkit.Where{
					{
						Field: "_id",
						Op:    gokvkit.WhereOpEq,
						Value: "1",
					},
				},
			})
			assert.NoError(b, err)
		}
	}))
}

// BenchmarkQuery2-12    	    2974	    392871 ns/op	  218961 B/op	    2691 allocs/op
func BenchmarkQuery2(b *testing.B) {
	b.ReportAllocs()
	assert.Nil(b, testutil.TestDB(func(ctx context.Context, db gokvkit.Database) {
		assert.NoError(b, seedDatabase(ctx, db))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := db.Query(ctx, "user", gokvkit.Query{
				Where: []gokvkit.Where{
					{
						Field: "age",
						Op:    gokvkit.WhereOpGt,
						Value: 50,
					},
				},
			})
			assert.NoError(b, err)
		}
	}))
}
