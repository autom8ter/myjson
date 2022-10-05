package wolverine_test

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/autom8ter/wolverine"
)

func TestCollection(t *testing.T) {
	schema, err := os.Open("./testdata/schemas/user.json")
	assert.Nil(t, err)
	bits, err := ioutil.ReadAll(schema)
	assert.Nil(t, err)
	usrCollection := &wolverine.Collection{
		Name: "user",
		Indexes: []wolverine.Index{
			{
				Fields:   []string{"contact.email"},
				FullText: true,
			},
		},
		JSONSchema: string(bits),
	}
	usr := newUserDoc()
	t.Run("validate", func(t *testing.T) {
		valid, err := usrCollection.Validate(usr)
		assert.Nil(t, err)
		assert.True(t, valid)
	})
}
