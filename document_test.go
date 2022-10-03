package wolverine

import (
	"testing"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/stretchr/testify/assert"
)

func TestDocument(t *testing.T) {
	type contact struct {
		Email string `json:"email"`
		Phone string `json:"phone,omitempty"`
	}
	type user struct {
		ID      string  `json:"id"`
		Contact contact `json:"contact"`
		Name    string  `json:"name"`
	}
	const email = "john.smith@yahoo.com"
	usr := user{ID: gofakeit.UUID(), Contact: contact{Email: email, Phone: gofakeit.Phone()}, Name: "john smith"}
	r, err := NewDocumentFromAny(&usr)
	if err != nil {
		t.Fatal(err)
	}
	r.SetID(usr.ID)
	r.SetCollection("user")
	t.Run("get id", func(t *testing.T) {
		assert.Equal(t, usr.ID, r.GetID())
	})
	t.Run("get email", func(t *testing.T) {
		assert.Equal(t, usr.Contact.Email, r.Get("contact.email"))
	})
	t.Run("get phone", func(t *testing.T) {
		assert.Equal(t, usr.Contact.Phone, r.Get("contact.phone"))
	})

	t.Run("merge", func(t *testing.T) {
		usr2 := user{ID: usr.ID, Contact: contact{Email: gofakeit.Email()}, Name: "john smith"}
		r2, err := NewDocumentFromAny(&usr2)
		if err != nil {
			t.Fatal(err)
		}
		r2.SetID(usr.ID)
		r2.SetCollection("user")
		r.Merge(r2)
		assert.Equal(t, usr2.Contact.Email, r.GetString("contact.email"))
		assert.Equal(t, usr.Contact.Phone, r.GetString("contact.phone"))
	})

}
