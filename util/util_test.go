package util_test

import (
	"bytes"
	"testing"

	"github.com/autom8ter/gokvkit"
	"github.com/autom8ter/gokvkit/testutil"
	"github.com/autom8ter/gokvkit/util"
	"github.com/stretchr/testify/assert"
)

func TestUtil(t *testing.T) {
	t.Run("yaml / json conversions", func(t *testing.T) {
		doc := testutil.NewUserDoc()
		yml, err := util.JSONToYAML([]byte(doc.String()))
		assert.Nil(t, err)
		jsonData, err := util.YAMLToJSON(yml)
		assert.Nil(t, err)
		doc2, err := gokvkit.NewDocumentFromBytes(jsonData)
		assert.Nil(t, err)
		assert.Equal(t, doc.String(), doc2.String())
	})
	t.Run("json string", func(t *testing.T) {
		doc := testutil.NewUserDoc()
		assert.Equal(t, doc.String(), util.JSONString(doc))
	})
	t.Run("decode", func(t *testing.T) {
		doc := testutil.NewUserDoc()
		data := map[string]any{}
		assert.Nil(t, util.Decode(doc.Value(), &data))
		doc2, err := gokvkit.NewDocumentFrom(data)
		assert.Nil(t, err)
		assert.Equal(t, doc.String(), doc2.String())
	})

	t.Run("validate", func(t *testing.T) {
		type usr struct {
			Name string `validate:"required"`
		}
		var u = usr{}
		assert.NotNil(t, util.ValidateStruct(&u))
		u.Name = "a name"
		assert.Nil(t, util.ValidateStruct(&u))
	})
	t.Run("encode value (float)", func(t *testing.T) {
		val1 := util.EncodeIndexValue(1.0)
		val2 := util.EncodeIndexValue(2.0)
		compare := bytes.Compare(val1, val2)
		assert.Equal(t, -1, compare)
	})
	t.Run("encode value (string)", func(t *testing.T) {
		val1 := util.EncodeIndexValue("hello")
		val2 := util.EncodeIndexValue("hellz")
		compare := bytes.Compare(val1, val2)
		assert.Equal(t, -1, compare)
	})
	t.Run("encode value (string)", func(t *testing.T) {
		val1 := util.EncodeIndexValue(false)
		val2 := util.EncodeIndexValue(true)
		compare := bytes.Compare(val1, val2)
		assert.Equal(t, -1, compare)
	})
	t.Run("encode value (json)", func(t *testing.T) {
		val1 := util.EncodeIndexValue(map[string]any{
			"message": "hello",
		})
		val2 := util.EncodeIndexValue(map[string]any{
			"message": "hellz",
		})
		compare := bytes.Compare(val1, val2)
		assert.Equal(t, -1, compare)
	})
	t.Run("encode value (empty)", func(t *testing.T) {
		val1 := util.EncodeIndexValue(nil)
		val2 := util.EncodeIndexValue(nil)
		compare := bytes.Compare(val1, val2)
		assert.Equal(t, 0, compare)
	})
}
