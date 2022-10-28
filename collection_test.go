package wolverine_test

import (
	"context"
	"github.com/autom8ter/wolverine"
	"github.com/autom8ter/wolverine/internal/testutil"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCollection(t *testing.T) {
	assert.Nil(t, testutil.TestDB(func(ctx context.Context, db *wolverine.DB) {
		collection := db.Collection("user")
		t.Run("schema", func(t *testing.T) {
			assert.NotNil(t, collection.Schema())
		})
		t.Run("db", func(t *testing.T) {
			assert.NotNil(t, collection.DB())
		})
		t.Run("schema primary query index", func(t *testing.T) {
			assert.NotNil(t, collection.Schema().PrimaryIndex())
		})
		t.Run("schema not empty", func(t *testing.T) {
			assert.NotEmpty(t, collection.Schema())
		})
		t.Run("schema name not empty", func(t *testing.T) {
			assert.NotEmpty(t, collection.Schema().Collection())
		})
	}))
}
