package gokvkit

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQueryBuilder(t *testing.T) {
	t.Run("query builder 1", func(t *testing.T) {
		q := Q().
			Select(Select{
				Field: "account_id",
			}).
			Where(Where{
				Field: "age",
				Op:    ">",
				Value: 50,
			}).
			GroupBy("account_id").
			OrderBy(OrderBy{
				Field:     "account_id",
				Direction: OrderByDirectionDesc,
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
