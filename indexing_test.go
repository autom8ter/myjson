package myjson

import (
	"context"
	"encoding/base64"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIndexing(t *testing.T) {
	t.Run("seekPrefix", func(t *testing.T) {
		{
			pfx := seekPrefix(context.Background(), "user", Index{
				Name:       "primary_idx",
				Fields:     []string{"_id"},
				Unique:     true,
				Primary:    true,
				ForeignKey: nil,
			}, map[string]any{
				"_id": "123",
			})

			assert.Equal(t,
				"ZGVmYXVsdABpbmRleAB1c2VyAHByaW1hcnlfaWR4AF9pZAAxMjM=",
				base64.StdEncoding.EncodeToString(pfx.Path()))
		}
		{
			pfx := seekPrefix(context.Background(), "user", Index{
				Name:       "primary_idx",
				Fields:     []string{"_id"},
				Unique:     true,
				Primary:    true,
				ForeignKey: nil,
			}, map[string]any{
				"_id": "123",
			})
			assert.Equal(t, 1, len(pfx.Fields()))
			assert.Equal(t,
				"ZGVmYXVsdABpbmRleAB1c2VyAHByaW1hcnlfaWR4AF9pZAAxMjMAMTIz",
				base64.StdEncoding.EncodeToString(pfx.Seek("123").Path()))
		}
		{
			pfx := seekPrefix(context.Background(), "user", Index{
				Name:       "testing",
				Fields:     []string{"account_id", "contact.email"},
				Unique:     false,
				Primary:    false,
				ForeignKey: nil,
			}, map[string]any{
				"account_id":    "123",
				"contact.email": "autom8ter@gmail.com",
			})
			assert.Equal(t, 2, len(pfx.Fields()))
			assert.Empty(t, pfx.SeekValue())
			assert.Equal(t,
				"ZGVmYXVsdABpbmRleAB1c2VyAHRlc3RpbmcAYWNjb3VudF9pZAAxMjMAY29udGFjdC5lbWFpbABhdXRvbTh0ZXJAZ21haWwuY29t",
				base64.StdEncoding.EncodeToString(pfx.Path()))
		}

	})
}
