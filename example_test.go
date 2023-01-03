package myjson_test

import (
	"context"
	"fmt"

	"github.com/autom8ter/myjson"
	"github.com/autom8ter/myjson/kv"
)

func ExampleNew() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	db, err := myjson.New(ctx, "badger", map[string]any{
		// leave empty for in-memory
		"storage_path": "",
	})
	if err != nil {
		panic(err)
	}
	defer db.Close(ctx)
	var accountSchema = `
type: object
x-collection: account
required:
  - _id
  - name
properties:
  _id:
    type: string
    description: The account's id.
    x-primary: true
  name:
    type: string
    description: The accounts's name.
`
	if err := db.ConfigureCollection(ctx, []byte(accountSchema)); err != nil {
		panic(err)
	}
	if err := db.Tx(ctx, kv.TxOpts{IsReadOnly: false}, func(ctx context.Context, tx myjson.Tx) error {
		// create a new account document
		account, err := myjson.NewDocumentFrom(map[string]any{
			"name": "acme.com",
		})
		if err != nil {
			return err
		}
		// create the account
		_, err = tx.Create(ctx, "account", account)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		panic(err)
	}
}

func ExampleQ() {
	query := myjson.Q().
		Select(myjson.Select{
			Field: "*",
		}).
		Where(myjson.Where{
			Field: "description",
			Op:    myjson.WhereOpContains,
			Value: "testing",
		}).Query()
	fmt.Println(query.String())
	// Output:
	// {"select":[{"field":"*"}],"where":[{"field":"description","op":"contains","value":"testing"}],"page":0}
}

func ExampleNewMetadata() {
	var orgID = "123"
	md := myjson.NewMetadata(map[string]any{})
	md.SetNamespace(orgID)
	bytes, _ := md.MarshalJSON()
	fmt.Println(string(bytes))
	// Output:
	// {"namespace":"123"}
}

func ExampleNewDocumentFrom() {
	doc, _ := myjson.NewDocumentFrom(map[string]any{
		"name": "autom8ter",
	})
	fmt.Println(doc.String())
	// Output:
	// {"name":"autom8ter"}
}
