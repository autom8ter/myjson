package wolverine_test

import (
	"testing"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/stretchr/testify/assert"

	"wolverine"
)

func TestRecord(t *testing.T) {
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
	r, err := wolverine.NewRecordFromStruct("user", usr.ID, &usr)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, usr.ID, r.GetID())
	t.Run("scan", func(t *testing.T) {
		var u user
		if err := r.Scan(&u); err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, usr.ID, u.ID)
		assert.Equal(t, usr.Contact.Email, u.Contact.Email)
	})
	t.Run("where", func(t *testing.T) {
		assert.Equal(t, true, r.Where([]wolverine.Where{
			{
				Field: "contact.email",
				Op:    "==",
				Value: email,
			},
		}))
		assert.Equal(t, true, r.Where([]wolverine.Where{
			{
				Field: "contact.email",
				Op:    "!=",
				Value: "",
			},
		}))
		assert.Equal(t, false, r.Where([]wolverine.Where{
			{
				Field: "contact.email",
				Op:    "==",
				Value: "",
			},
		}))
	})
	t.Run("select", func(t *testing.T) {
		selected := r.Select([]string{"name", "contact.email"})
		assert.NotEmpty(t, selected["name"])
		assert.NotEmpty(t, selected["contact.email"])
	})
}
