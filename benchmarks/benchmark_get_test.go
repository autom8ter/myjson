package benchmarks

import (
	"context"
	"testing"

	"github.com/autom8ter/gokvkit"
	"github.com/autom8ter/gokvkit/testutil"
	"github.com/stretchr/testify/assert"
)

// BenchmarkGet-12    	    5100	    221980 ns/op	  174096 B/op	    2198 allocs/op
func BenchmarkGet(b *testing.B) {
	b.ReportAllocs()
	assert.Nil(b, testutil.TestDB(func(ctx context.Context, db gokvkit.Database) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := db.Get(ctx, "account", "1")
			assert.NoError(b, err)
		}
	}))
}
