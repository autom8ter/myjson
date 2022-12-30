package kv_test

import (
	"testing"

	_ "github.com/autom8ter/gokvkit/kv/badger"
	"github.com/autom8ter/gokvkit/kv/registry"
	"github.com/stretchr/testify/assert"
)

func Test(t *testing.T) {
	var providers = []string{"badger"}
	for _, provider := range providers {
		t.Run(provider, func(t *testing.T) {
			{
				db, err := registry.Open(provider, map[string]interface{}{
					"storage_path": "",
				})
				assert.NoError(t, err)
				assert.NoError(t, db.Close())
			}
		})
	}
}
