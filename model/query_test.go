package model

import (
	"context"
	"github.com/autom8ter/gokvkit/internal/util"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestQuery(t *testing.T) {
	t.Run("validate empty query", func(t *testing.T) {
		q := QueryJson{
			From:   "",
			Select: nil,
			Where:  nil,
		}
		assert.NotNil(t, q.Validate(context.Background()))
	})
	t.Run("validate no from", func(t *testing.T) {
		a := QueryJson{
			From: "",
		}
		assert.NotNil(t, a.Validate(context.Background()))
	})
	t.Run("validate no select", func(t *testing.T) {
		a := QueryJson{
			From: "testing",
		}
		assert.NotNil(t, a.Validate(context.Background()))
	})
	t.Run("validate bad group by", func(t *testing.T) {
		a := QueryJson{
			From:    "user",
			GroupBy: []string{"account_id"},
			Select: []QueryJsonSelectElem{
				{
					Field:     "age",
					Aggregate: util.ToPtr(QueryJsonSelectElemAggregateSum),
					As:        util.ToPtr("age_sum"),
				},
			},
			OrderBy: []QueryJsonOrderByElem{
				{
					Field:     "account_id",
					Direction: QueryJsonOrderByElemDirectionAsc,
				},
			},
		}
		assert.NotNil(t, a.Validate(context.Background()))
	})
	t.Run("validate good query", func(t *testing.T) {
		a := QueryJson{
			From: "testing",
			Select: []QueryJsonSelectElem{
				{
					Field:     "test",
					Aggregate: util.ToPtr(QueryJsonSelectElemAggregateMax),
					As:        util.ToPtr("max_test"),
				},
			},
		}
		assert.Nil(t, a.Validate(context.Background()))
	})
}
