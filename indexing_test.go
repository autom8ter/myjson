package gokvkit

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/autom8ter/gokvkit/model"
	"github.com/brianvoe/gofakeit/v6"
	"github.com/stretchr/testify/assert"
)

func TestIndexing(t *testing.T) {
	i := &model.Index{
		Collection: "user",
		Name:       "user_account_email_idx",
		Fields:     []string{"account_id", "contact.email"},
		Unique:     true,
		Primary:    false,
	}
	u := newUserDoc()
	prefix := i.SeekPrefix(u.Value())
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

func newUserDoc() *model.Document {
	doc, err := model.NewDocumentFrom(map[string]interface{}{
		"_id":  gofakeit.UUID(),
		"name": gofakeit.Name(),
		"contact": map[string]interface{}{
			"email": fmt.Sprintf("%v.%s", gofakeit.IntRange(0, 100), gofakeit.Email()),
		},
		"account_id":      gofakeit.IntRange(0, 100),
		"language":        gofakeit.Language(),
		"birthday_month":  gofakeit.Month(),
		"favorite_number": gofakeit.Second(),
		"gender":          gofakeit.Gender(),
		"age":             gofakeit.IntRange(0, 100),
		"timestamp":       gofakeit.DateRange(time.Now().Truncate(7200*time.Hour), time.Now()),
		"annotations":     gofakeit.Map(),
	})
	if err != nil {
		panic(err)
	}
	return doc
}
