package myjson

import (
	"testing"
	"time"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/stretchr/testify/assert"
)

func TestOptimizer(t *testing.T) {
	o := defaultOptimizer{}
	schema, err := newCollectionSchema([]byte(userSchema))
	assert.NoError(t, err)
	indexes := schema
	t.Run("select secondary index", func(t *testing.T) {
		explain, err := o.Optimize(indexes, []Where{
			{
				Field: "contact.email",
				Op:    WhereOpEq,
				Value: gofakeit.Email(),
			},
		})
		assert.NoError(t, err)
		assert.Equal(t, false, explain.Index.Primary)
		assert.Equal(t, "contact.email", explain.MatchedFields[0])
	})

	t.Run("select primary index", func(t *testing.T) {
		explain, err := o.Optimize(indexes, []Where{
			{
				Field: "_id",
				Op:    WhereOpEq,
				Value: gofakeit.Email(),
			},
		})
		assert.NoError(t, err)
		assert.Equal(t, true, explain.Index.Primary, explain.MatchedFields)
		assert.Equal(t, "_id", explain.MatchedFields[0], explain.MatchedFields)
	})

	t.Run("select secondary index (multi-field)", func(t *testing.T) {
		explain, err := o.Optimize(indexes, []Where{
			{
				Field: "account_id",
				Op:    WhereOpEq,
				Value: "1",
			},
			{
				Field: "contact.email",
				Op:    WhereOpEq,
				Value: gofakeit.Email(),
			},
		})
		assert.NoError(t, err)
		assert.Equal(t, false, explain.Index.Primary)
		assert.Equal(t, "account_id", explain.MatchedFields[0])
		assert.Equal(t, "contact.email", explain.MatchedFields[1])
	})
	t.Run("select secondary index 2", func(t *testing.T) {
		explain, err := o.Optimize(indexes, []Where{
			{
				Field: "contact.email",
				Op:    WhereOpEq,
				Value: gofakeit.Email(),
			},
			{
				Field: "account_id",
				Op:    WhereOpEq,
				Value: "1",
			},
		})
		assert.NoError(t, err)
		assert.EqualValues(t, false, explain.Index.Primary)
		assert.Equal(t, "contact.email", explain.MatchedFields[0])
	})
	t.Run("select secondary index (multi-field partial match)", func(t *testing.T) {
		explain, err := o.Optimize(indexes, []Where{
			{
				Field: "account_id",
				Op:    WhereOpEq,
				Value: "1",
			},
		})
		assert.NoError(t, err)
		assert.Equal(t, false, explain.Index.Primary)
		assert.Equal(t, "account_id", explain.MatchedFields[0])
	})
	t.Run("select secondary index (multi-field partial match (!=))", func(t *testing.T) {
		explain, err := o.Optimize(indexes, []Where{
			{
				Field: "account_id",
				Op:    "!=",
				Value: "1",
			},
		})
		assert.NoError(t, err)
		assert.Equal(t, true, explain.Index.Primary)
		assert.Equal(t, 0, len(explain.MatchedFields))
	})
	t.Run("select secondary index (>)", func(t *testing.T) {
		cdc, err := newCollectionSchema([]byte(cdcSchema))
		assert.NoError(t, err)
		explain, err := o.Optimize(cdc, []Where{
			{
				Field: "timestamp",
				Op:    WhereOpGt,
				Value: time.Now().String(),
			},
		})
		assert.NoError(t, err)
		assert.Equal(t, false, explain.Index.Primary)
		assert.Equal(t, "timestamp", explain.SeekFields[0])
		assert.NotEmpty(t, explain.SeekValues["timestamp"])
	})
	t.Run("select primary index (neq)", func(t *testing.T) {
		explain, err := o.Optimize(indexes, []Where{
			{
				Field: "_id",
				Op:    WhereOpNeq,
				Value: gofakeit.Email(),
			},
		})
		assert.NoError(t, err)
		assert.Equal(t, true, explain.Index.Primary)
	})
	t.Run("select primary index (hasPrefix)", func(t *testing.T) {
		explain, err := o.Optimize(indexes, []Where{
			{
				Field: "_id",
				Op:    WhereOpHasPrefix,
				Value: gofakeit.Email(),
			},
		})
		assert.NoError(t, err)
		assert.Equal(t, true, explain.Index.Primary)
	})
	t.Run("select primary index (hasSuffix)", func(t *testing.T) {
		explain, err := o.Optimize(indexes, []Where{
			{
				Field: "_id",
				Op:    WhereOpHasSuffix,
				Value: gofakeit.Email(),
			},
		})
		assert.NoError(t, err)
		assert.Equal(t, true, explain.Index.Primary)
	})
	t.Run("select primary index (contains)", func(t *testing.T) {
		explain, err := o.Optimize(indexes, []Where{
			{
				Field: "_id",
				Op:    WhereOpContains,
				Value: gofakeit.Email(),
			},
		})
		assert.NoError(t, err)
		assert.Equal(t, true, explain.Index.Primary)
	})
	t.Run("select primary index (in)", func(t *testing.T) {
		explain, err := o.Optimize(indexes, []Where{
			{
				Field: "_id",
				Op:    WhereOpIn,
				Value: []string{gofakeit.Email()},
			},
		})
		assert.NoError(t, err)
		assert.Equal(t, true, explain.Index.Primary)
	})
	t.Run("select primary index (containsAll)", func(t *testing.T) {
		explain, err := o.Optimize(indexes, []Where{
			{
				Field: "_id",
				Op:    WhereOpContainsAll,
				Value: []string{gofakeit.Email()},
			},
		})
		assert.NoError(t, err)
		assert.Equal(t, true, explain.Index.Primary)
	})
}
