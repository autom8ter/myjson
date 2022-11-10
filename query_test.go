package brutus

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestQuery(t *testing.T) {
	t.Run("query", func(t *testing.T) {
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
}
