package wolverine_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/autom8ter/wolverine"
)

func TestCache(t *testing.T) {
	t.Run("set get del", func(t *testing.T) {
		assert.Nil(t, testDB(defaultCollections, func(ctx context.Context, db wolverine.DB) {
			assert.Nil(t, db.SetCache("hello", "world", time.Time{}))
			value, err := db.GetCache("hello")
			assert.Nil(t, err)
			assert.Equal(t, "world", value)
			assert.Nil(t, db.DelCache("hello"))
			_, err = db.GetCache("hello")
			assert.NotNil(t, err)
		}))
	})

}
