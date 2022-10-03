package wolverine

import (
	"testing"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/stretchr/testify/assert"
)

func TestDocument(t *testing.T) {
	type contact struct {
		Email string `json:"email"`
	}
	type user struct {
		ID      string  `json:"id"`
		Contact contact `json:"contact"`
		Name    string  `json:"name"`
	}
	const email = "john.smith@yahoo.com"
	usr := user{ID: gofakeit.UUID(), Contact: contact{Email: email}, Name: "john smith"}
	r, err := NewDocumentFromAny(&usr)
	if err != nil {
		t.Fatal(err)
	}
	r.SetID(usr.ID)
	r.SetCollection("user")
	assert.Equal(t, usr.ID, r.GetID())
	assert.Equal(t, usr.Contact.Email, r.Get("contact.email"))
}
