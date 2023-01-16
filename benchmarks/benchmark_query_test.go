package benchmarks

import (
	"context"
	"testing"

	"github.com/autom8ter/myjson"
	"github.com/autom8ter/myjson/testutil"
	"github.com/stretchr/testify/assert"
)

// BenchmarkQuery-12    	    4530	    290003 ns/op	  183447 B/op	    2365 allocs/op
func BenchmarkQuery(b *testing.B) {
	b.ReportAllocs()
	assert.Nil(b, testutil.TestDB(func(ctx context.Context, db myjson.Database) {
		assert.NoError(b, testutil.SeedUsers(ctx, db, 10, 3))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := db.Query(ctx, "account", myjson.Query{
				Where: []myjson.Where{
					{
						Field: "_id",
						Op:    myjson.WhereOpEq,
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
	assert.Nil(b, testutil.TestDB(func(ctx context.Context, db myjson.Database) {
		assert.NoError(b, testutil.SeedUsers(ctx, db, 10, 3))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := db.Query(ctx, "user", myjson.Query{
				Where: []myjson.Where{
					{
						Field: "age",
						Op:    myjson.WhereOpGt,
						Value: 50,
					},
				},
			})
			assert.NoError(b, err)
		}
	}))
}
