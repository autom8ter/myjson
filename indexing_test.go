package gokvkit_test

import (
	"bytes"
	"github.com/autom8ter/gokvkit"
	"github.com/autom8ter/gokvkit/testutil"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestIndexing(t *testing.T) {
	i := &gokvkit.Index{
		Collection: "user",
		Name:       "user_account_email_idx",
		Fields:     []string{"account_id", "contact.email"},
		Unique:     true,
		Primary:    false,
	}
	u := testutil.NewUserDoc()
	prefix := i.Seek(u.Value())
	t.Log(string(prefix.Path()))
	t.Run("fields", func(t *testing.T) {
		fields := prefix.Fields()
		assert.Equal(t, fields[0].Field, "account_id")
		assert.Equal(t, fields[0].Value, u.GetFloat("account_id"))
		assert.Equal(t, fields[1].Field, "contact.email")
		assert.Equal(t, fields[1].Value, u.GetString("contact.email"))
	})
	t.Run("setDocumentID", func(t *testing.T) {
		beforePath := prefix.Path()
		prefix = prefix.SetDocumentID(u.GetString("_id"))
		assert.Equal(t, u.GetString("_id"), prefix.DocumentID())
		afterPath := prefix.Path()
		assert.Equal(t, 1, bytes.Compare(afterPath, beforePath))
	})
	t.Run("append", func(t *testing.T) {
		prefix = prefix.Append("language", "english")
		assert.Equal(t, "language", prefix.Fields()[len(prefix.Fields())-1].Field)
		assert.Equal(t, "english", prefix.Fields()[len(prefix.Fields())-1].Value)
	})

}
