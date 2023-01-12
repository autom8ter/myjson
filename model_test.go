package myjson

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQuery(t *testing.T) {
	t.Run("validate empty query", func(t *testing.T) {
		q := Query{
			Select: nil,
			Where:  nil,
		}
		assert.NotNil(t, q.Validate(context.Background()))
	})
	t.Run("validate no select", func(t *testing.T) {
		a := Query{}
		assert.NotNil(t, a.Validate(context.Background()))
	})
	t.Run("validate bad group by", func(t *testing.T) {
		a := Query{
			GroupBy: []string{"account_id"},
			Select: []Select{
				{
					Field:     "age",
					Aggregate: AggregateFunctionSum,
					As:        "age_sum",
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
	t.Run("validate bad order by", func(t *testing.T) {
		a := Query{
			Select: []Select{
				{
					Field: "*",
				},
			},
			OrderBy: []OrderBy{
				{
					Field:     "account_id",
					Direction: "dsc",
				},
			},
		}
		assert.NotNil(t, a.Validate(context.Background()))
	})
	t.Run("validate bad where op", func(t *testing.T) {
		a := Query{
			Select: []Select{
				{
					Field: "*",
				},
			},
			Where: []Where{
				{
					Field: "account_id",
					Op:    "==",
					Value: 9,
				},
			},
		}
		assert.NotNil(t, a.Validate(context.Background()))
	})
	t.Run("validate bad where field", func(t *testing.T) {
		a := Query{
			Select: []Select{
				{
					Field: "*",
				},
			},
			Where: []Where{
				{
					Field: "",
					Op:    WhereOpEq,
					Value: 9,
				},
			},
		}
		assert.NotNil(t, a.Validate(context.Background()))
	})
	t.Run("validate bad where value", func(t *testing.T) {
		a := Query{
			Select: []Select{
				{
					Field: "*",
				},
			},
			Where: []Where{
				{
					Field: "name",
					Op:    WhereOpEq,
				},
			},
		}
		assert.NotNil(t, a.Validate(context.Background()))
	})
	t.Run("validate bad limit", func(t *testing.T) {
		a := Query{
			Select: []Select{
				{
					Field: "*",
				},
			},
			Limit: -1,
		}
		assert.NotNil(t, a.Validate(context.Background()))
	})
	t.Run("validate bad page", func(t *testing.T) {
		a := Query{
			Select: []Select{
				{
					Field: "*",
				},
			},
			Page: -1,
		}
		assert.NotNil(t, a.Validate(context.Background()))
	})
	t.Run("validate good query", func(t *testing.T) {
		a := Query{
			Select: []Select{
				{
					Field:     "test",
					Aggregate: AggregateFunctionMax,
					As:        "max_test",
				},
			},
		}
		assert.Nil(t, a.Validate(context.Background()))
	})
}

func TestMetadataContext(t *testing.T) {
	ctx := context.Background()
	c, ok := GetMetadata(ctx)
	assert.False(t, ok)
	assert.NotNil(t, c)
	c = NewMetadata()
	c.Set("testing", true)
	ctx = c.ToContext(ctx)
	c, ok = GetMetadata(ctx)
	assert.True(t, ok)
	assert.NotNil(t, c)

	c.SetNamespace("acme.com")
	assert.Equal(t, "acme.com", c.GetNamespace())
}
