package gokvkit

import (
	"github.com/autom8ter/gokvkit/model"
	"github.com/brianvoe/gofakeit/v6"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestOptimizer(t *testing.T) {
	o := defaultOptimizer{}
	indexes := map[string]model.Index{
		"primary_idx": {
			Collection: "testing",
			Name:       "primary_idx",
			Fields:     []string{"_id"},
			Unique:     true,
			Primary:    true,
		},
		"email_idx": {
			Collection: "testing",
			Name:       "email_idx",
			Fields:     []string{"email"},
			Unique:     true,
			Primary:    false,
		},
		"account_language_idx": {
			Collection: "testing",
			Name:       "account_language_idx",
			Fields:     []string{"account_id", "language"},
			Unique:     false,
			Primary:    false,
		},
	}
	t.Run("select secondary index", func(t *testing.T) {
		i, err := o.Optimize(indexes, []model.QueryJsonWhereElem{
			{
				Field: "email",
				Op:    "==",
				Value: gofakeit.Email(),
			},
		})
		assert.Nil(t, err)
		assert.Equal(t, false, i.IsPrimaryIndex)
		assert.Equal(t, "email", i.MatchedFields[0])
	})

	t.Run("select primary index", func(t *testing.T) {
		i, err := o.Optimize(indexes, []model.QueryJsonWhereElem{
			{
				Field: "_id",
				Op:    "==",
				Value: gofakeit.Email(),
			},
		})
		assert.Nil(t, err)
		assert.Equal(t, true, i.IsPrimaryIndex, i.MatchedFields)
		assert.Equal(t, "_id", i.MatchedFields[0], i.MatchedFields)
	})

	t.Run("select secondary index (multi-field)", func(t *testing.T) {
		i, err := o.Optimize(indexes, []model.QueryJsonWhereElem{
			{
				Field: "account_id",
				Op:    "==",
				Value: 1,
			},
			{
				Field: "language",
				Op:    "==",
				Value: gofakeit.Language(),
			},
		})
		assert.Nil(t, err)
		assert.Equal(t, false, i.IsPrimaryIndex)
		assert.Equal(t, "account_id", i.MatchedFields[0])
		assert.Equal(t, "language", i.MatchedFields[1])
	})
	t.Run("select secondary index (multi-field wrong order)", func(t *testing.T) {
		i, err := o.Optimize(indexes, []model.QueryJsonWhereElem{
			{
				Field: "language",
				Op:    "==",
				Value: gofakeit.Language(),
			},
			{
				Field: "account_id",
				Op:    "==",
				Value: 1,
			},
		})
		assert.Nil(t, err)
		assert.Equal(t, true, i.IsPrimaryIndex)
	})
	t.Run("select secondary index (multi-field partial match)", func(t *testing.T) {
		i, err := o.Optimize(indexes, []model.QueryJsonWhereElem{
			{
				Field: "account_id",
				Op:    "==",
				Value: 1,
			},
		})
		assert.Nil(t, err)
		assert.Equal(t, false, i.IsPrimaryIndex)
		assert.Equal(t, "account_id", i.MatchedFields[0])
	})
	t.Run("select secondary index (multi-field partial match (!=))", func(t *testing.T) {
		i, err := o.Optimize(indexes, []model.QueryJsonWhereElem{
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
