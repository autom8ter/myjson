package gokvkit

import (
	"testing"

	"github.com/autom8ter/gokvkit/model"
	"github.com/stretchr/testify/assert"
)

func TestQuery(t *testing.T) {
	t.Run("query builder 1", func(t *testing.T) {
		q := NewQueryBuilder().
			Select(model.Select{
				Field: "account_id",
			}).
			Where(model.Where{
				Field: "age",
				Op:    ">",
				Value: 50,
			}).
			GroupBy("account_id").
			OrderBy(model.OrderBy{
				Field:     "account_id",
				Direction: model.OrderByDirectionDesc,
			}).
			Limit(1).
			Query()
		assert.Equal(t, 1, len(q.Select))
		assert.Equal(t, 1, len(q.Where))
		assert.Equal(t, 1, len(q.GroupBy))
		assert.Equal(t, 1, len(q.OrderBy))
		assert.Equal(t, 1, q.Limit)
	})
}
