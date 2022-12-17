package gokvkit

import (
	"testing"

	"github.com/autom8ter/gokvkit/model"
	"github.com/brianvoe/gofakeit/v6"
	"github.com/stretchr/testify/assert"
)

func TestOptimizer(t *testing.T) {
	o := defaultOptimizer{}
	schema, err := newCollectionSchema([]byte(userSchema))
	assert.Nil(t, err)
	indexes := schema
	t.Run("select secondary index", func(t *testing.T) {
		i, err := o.Optimize(indexes, []model.Where{
			{
				Field: "contact.email",
				Op:    model.WhereOpEq,
				Value: gofakeit.Email(),
			},
		})
		assert.Nil(t, err)
		assert.Equal(t, false, i.IsPrimaryIndex)
		assert.Equal(t, "contact.email", i.MatchedFields[0])
	})

	t.Run("select primary index", func(t *testing.T) {
		i, err := o.Optimize(indexes, []model.Where{
			{
				Field: "_id",
				Op:    model.WhereOpEq,
				Value: gofakeit.Email(),
			},
		})
		assert.Nil(t, err)
		assert.Equal(t, true, i.IsPrimaryIndex, i.MatchedFields)
		assert.Equal(t, "_id", i.MatchedFields[0], i.MatchedFields)
	})

	t.Run("select secondary index (multi-field)", func(t *testing.T) {
		i, err := o.Optimize(indexes, []model.Where{
			{
				Field: "account_id",
				Op:    model.WhereOpEq,
				Value: 1,
			},
			{
				Field: "contact.email",
				Op:    model.WhereOpEq,
				Value: gofakeit.Email(),
			},
		})
		assert.Nil(t, err)
		assert.Equal(t, false, i.IsPrimaryIndex)
		assert.Equal(t, "account_id", i.MatchedFields[0])
		assert.Equal(t, "contact.email", i.MatchedFields[1])
	})
	t.Run("select secondary index 2", func(t *testing.T) {
		i, err := o.Optimize(indexes, []model.Where{
			{
				Field: "contact.email",
				Op:    model.WhereOpEq,
				Value: gofakeit.Email(),
			},
			{
				Field: "account_id",
				Op:    model.WhereOpEq,
				Value: 1,
			},
		})
		assert.Nil(t, err)
		assert.EqualValues(t, false, i.IsPrimaryIndex)
		assert.Equal(t, "contact.email", i.MatchedFields[0])
	})
	t.Run("select secondary index (multi-field partial match)", func(t *testing.T) {
		i, err := o.Optimize(indexes, []model.Where{
			{
				Field: "account_id",
				Op:    model.WhereOpEq,
				Value: 1,
			},
		})
		assert.Nil(t, err)
		assert.Equal(t, false, i.IsPrimaryIndex)
		assert.Equal(t, "account_id", i.MatchedFields[0])
	})
	t.Run("select secondary index (multi-field partial match (!=))", func(t *testing.T) {
		i, err := o.Optimize(indexes, []model.Where{
			{
				Field: "account_id",
				Op:    "!=",
				Value: 1,
			},
		})
		assert.Nil(t, err)
		assert.Equal(t, true, i.IsPrimaryIndex)
		assert.Equal(t, 0, len(i.MatchedFields))
	})
}
