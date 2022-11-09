package brutus

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
		assert.Equal(t, "_id", i.MatchedFields[0])
		assert.Equal(t, true, i.IsPrimaryIndex)
	})
	t.Run("select secondary index order by", func(t *testing.T) {
		i, err := o.BestIndex(indexes, []Where{}, OrderBy{
			Field:     "email",
			Direction: DESC,
		})
		assert.Nil(t, err)
		//assert.Equal(t, "email", i.MatchedFields[0])
		assert.Equal(t, true, i.IsOrdered)
	})
}
