package gokvkit

import (
	"github.com/brianvoe/gofakeit/v6"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestOptimizer(t *testing.T) {
	o := defaultOptimizer{}
	indexes := map[string]Index{
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
		i, err := o.BestIndex(indexes, []Where{
			{
				Field: "email",
				Op:    "==",
				Value: gofakeit.Email(),
			},
		}, OrderBy{})
		assert.Nil(t, err)
		assert.Equal(t, false, i.IsPrimaryIndex)
		assert.Equal(t, "email", i.MatchedFields[0])
	})

	t.Run("select primary index", func(t *testing.T) {
		i, err := o.BestIndex(indexes, []Where{
			{
				Field: "_id",
				Op:    "==",
				Value: gofakeit.Email(),
			},
		}, OrderBy{})
		assert.Nil(t, err)
		assert.Equal(t, true, i.IsPrimaryIndex, i.MatchedFields)
		assert.Equal(t, "_id", i.MatchedFields[0], i.MatchedFields)
	})
	t.Run("select secondary index order by", func(t *testing.T) {
		i, err := o.BestIndex(indexes, []Where{}, OrderBy{
			Field:     "email",
			Direction: DESC,
		})
		assert.Nil(t, err)
		assert.Equal(t, "email", i.MatchedFields[0])
		assert.Equal(t, true, i.IsOrdered)
	})
	t.Run("select secondary index order by (partial match)", func(t *testing.T) {
		i, err := o.BestIndex(indexes, []Where{}, OrderBy{
			Field:     "account_id",
			Direction: DESC,
		})
		assert.Nil(t, err)
		assert.Equal(t, "account_id", i.MatchedFields[0])
		assert.Equal(t, true, i.IsOrdered)
	})
	t.Run("select secondary index (multi-field)", func(t *testing.T) {
		i, err := o.BestIndex(indexes, []Where{
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
		}, OrderBy{})
		assert.Nil(t, err)
		assert.Equal(t, false, i.IsPrimaryIndex)
		assert.Equal(t, "account_id", i.MatchedFields[0])
		assert.Equal(t, "language", i.MatchedFields[1])
	})
	t.Run("select secondary index (multi-field wrong order)", func(t *testing.T) {
		i, err := o.BestIndex(indexes, []Where{
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
		}, OrderBy{})
		assert.Nil(t, err)
		assert.Equal(t, true, i.IsPrimaryIndex)
	})
	t.Run("select secondary index (multi-field partial match)", func(t *testing.T) {
		i, err := o.BestIndex(indexes, []Where{
			{
				Field: "account_id",
				Op:    "==",
				Value: 1,
			},
		}, OrderBy{})
		assert.Nil(t, err)
		assert.Equal(t, false, i.IsPrimaryIndex)
		assert.Equal(t, "account_id", i.MatchedFields[0])
	})
	t.Run("select secondary index (multi-field partial match (!=))", func(t *testing.T) {
		i, err := o.BestIndex(indexes, []Where{
			{
				Field: "account_id",
				Op:    "!=",
				Value: 1,
			},
		}, OrderBy{})
		assert.Nil(t, err)
		assert.Equal(t, true, i.IsPrimaryIndex)
		assert.NotEqual(t, "account_id", i.MatchedFields[0])
	})
}
