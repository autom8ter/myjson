package model

import (
	"context"
	"github.com/autom8ter/gokvkit/internal/util"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestQuery(t *testing.T) {
	t.Run("validate empty query", func(t *testing.T) {
		q := Query{
			From:   "",
			Select: nil,
			Where:  nil,
		}
		assert.NotNil(t, q.Validate(context.Background()))
	})
	t.Run("validate no from", func(t *testing.T) {
		a := Query{
			From: "",
		}
		assert.NotNil(t, a.Validate(context.Background()))
	})
	t.Run("validate no select", func(t *testing.T) {
		a := Query{
			From: "testing",
		}
		assert.NotNil(t, a.Validate(context.Background()))
	})
	t.Run("validate bad group by", func(t *testing.T) {
		a := Query{
			From:    "user",
			GroupBy: []string{"account_id"},
			Select: []Select{
				{
					Field:     "age",
					Aggregate: util.ToPtr(SelectAggregateSum),
					As:        util.ToPtr("age_sum"),
				},
			},
			OrderBy: []OrderBy{
				{
					Field:     "account_id",
					Direction: OrderByDirectionAsc,
				},
			},
		}
		assert.NotNil(t, a.Validate(context.Background()))
	})
	t.Run("validate good query", func(t *testing.T) {
		a := Query{
			From: "testing",
			Select: []Select{
				{
					Field:     "test",
					Aggregate: util.ToPtr(SelectAggregateMax),
					As:        util.ToPtr("max_test"),
				},
			},
		}
		assert.Nil(t, a.Validate(context.Background()))
	})
}
