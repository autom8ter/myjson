package gokvkit

import (
	"github.com/autom8ter/gokvkit/model"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestQuery(t *testing.T) {
	t.Run("query builder 1", func(t *testing.T) {
		q := NewQueryBuilder().
			Select(model.QueryJsonSelectElem{
				Field: "account_id",
			}).
			Where(model.QueryJsonWhereElem{
				Field: "age",
				Op:    ">",
				Value: 50,
			}).
			GroupBy("account_id").
			OrderBy(model.QueryJsonOrderByElem{
				Field:     "account_id",
				Direction: model.QueryJsonOrderByElemDirectionDesc,
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
