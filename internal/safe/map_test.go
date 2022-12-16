package safe_test

import (
	"fmt"
	"testing"

	"github.com/autom8ter/gokvkit/internal/safe"
	"github.com/spf13/cast"
	"github.com/stretchr/testify/assert"
)

func Test(t *testing.T) {
	m := safe.Map[map[string]any]{}
	assert.False(t, m.Exists("1"))
	for i := 0; i < 10; i++ {
		m.Set(fmt.Sprint(i), map[string]any{
			"value": i,
		})
	}
	for i := 0; i < 10; i++ {
		assert.True(t, m.Exists(fmt.Sprint(i)))
		entry := m.Get(fmt.Sprint(i))
		assert.Equal(t, entry["value"], i)
	}
	m.Range(func(key string, entry map[string]any) bool {
		assert.Equal(t, entry["value"], cast.ToInt(key))
		return true
	})

	for i := 0; i < 10; i++ {
		m.Del(fmt.Sprint(i))
	}
	for i := 0; i < 10; i++ {
		assert.False(t, m.Exists(fmt.Sprint(i)))
	}
	m.SetFunc("setFunc", func(_ map[string]any) map[string]any {
		return map[string]any{
			"message": "hello world",
		}
	})
	assert.Equal(t, "hello world", m.Get("setFunc")["message"])
}
