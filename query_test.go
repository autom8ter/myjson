package gokvkit

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestQuery(t *testing.T) {
	t.Run("validate empty query", func(t *testing.T) {
		q := Query{
			From:    "",
			Select:  nil,
			Where:   nil,
			Page:    0,
			Limit:   0,
			OrderBy: OrderBy{},
		}
		assert.NotNil(t, q.Validate())
	})
	t.Run("validate no from", func(t *testing.T) {
		a := Query{
			From: "",
		}
		assert.NotNil(t, a.Validate())
	})
	t.Run("validate no select", func(t *testing.T) {
		a := Query{
			From: "testing",
		}
		assert.NotNil(t, a.Validate())
	})
	t.Run("validate bad group by", func(t *testing.T) {
		a := Query{
			From:    "user",
			GroupBy: []string{"account_id"},
			Select: []SelectField{
				{
					Field:    "age",
					Function: SUM,
					As:       "age_sum",
				},
			},
			Page:  0,
			Limit: 0,
			OrderBy: OrderBy{
				Field:     "account_id",
				Direction: ASC,
			},
		}
		assert.NotNil(t, a.Validate())
	})
	t.Run("validate good query", func(t *testing.T) {
		a := Query{
			From: "testing",
			Select: []SelectField{
				{
					Field:    "test",
					Function: MAX,
					As:       "max_test",
				},
			},
		}
		assert.Nil(t, a.Validate())
	})
}
