package myjson_test

import (
	"context"
	"fmt"

	"github.com/autom8ter/myjson"
	"github.com/autom8ter/myjson/kv"
)

func ExampleOpen() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	db, err := myjson.Open(ctx, "badger", map[string]any{
		// leave empty for in-memory
		"storage_path": "",
	})
	if err != nil {
		panic(err)
	}
	defer db.Close(ctx)
	var accountSchema = `
type: object
# collection name
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
x-authorization:
  rules:
    ## allow super users to do anything
    - effect: allow
      ## match on any action
      action:
      - "*"
      ## context metadata must have is_super_user set to true
      match: |
        contains(meta.Get('roles'), 'super_user')

      ## dont allow read-only users to create/update/delete/set accounts
    - effect: deny
      ## match on document mutations
      action:
        - create
        - update
        - delete
        - set
        ## context metadata must have is_read_only set to true
      match: |
        contains(meta.Get('roles'), 'read_only')

      ## only allow users to update their own account
    - effect: allow
        ## match on document mutations
      action:
        - create
        - update
        - delete
        - set
        ## the account's _id must match the user's account_id
      match: |
        doc.Get('_id') == meta.Get('account_id')

      ## only allow users to query their own account
    - effect: allow
        ## match on document queries (includes ForEach and other Query based methods)
      action:
        - query
        ## user must have a group matching the account's _id
      match: |
        query.where?.length > 0 && query.where[0].field == '_id' && query.where[0].op == 'eq' && contains(meta.Get('groups'), query.where[0].value) 

`
	if err := db.Configure(ctx, map[string]string{
		"account": accountSchema,
		// "user": userSchema,
		// "task": taskSchema,
	}); err != nil {
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

func ExampleD() {
	doc := myjson.D().Set(map[string]any{
		"name":  "John Doe",
		"email": "johndoe@gmail.com",
	}).Doc()
	fmt.Println(doc.String())
}

func ExampleSetMetadataGroups() {
	ctx := context.Background()
	ctx = myjson.SetMetadataGroups(ctx, []string{"group1", "group2"})
	fmt.Println(myjson.ExtractMetadata(ctx).GetArray("groups"))
}

func ExampleSetMetadataRoles() {
	ctx := context.Background()
	ctx = myjson.SetMetadataRoles(ctx, []string{"super_user"})
	fmt.Println(myjson.ExtractMetadata(ctx).GetArray("roles"))
}

func ExampleExtractMetadata() {
	ctx := context.Background()
	meta := myjson.ExtractMetadata(ctx)
	fmt.Println(meta.String())
	// Output:
	// {"namespace":"default"}
}

func ExampleNewDocumentFrom() {
	doc, _ := myjson.NewDocumentFrom(map[string]any{
		"name": "autom8ter",
	})
	fmt.Println(doc.String())
	// Output:
	// {"name":"autom8ter"}
}

func ExampleDocument_Scan() {
	type User struct {
		Name string `json:"name"`
	}
	doc, _ := myjson.NewDocumentFrom(map[string]any{
		"name": "autom8ter",
	})

	var usr User

	doc.Scan(&usr)
	fmt.Println(usr.Name)
	// Output:
	// autom8ter
}

func ExampleDocument_Set() {

	doc := myjson.NewDocument()
	doc.Set("name", "autom8ter")
	doc.Set("contact.email", "coleman@autom8ter.com")

	fmt.Println(doc.String())
	// Output:
	// {"name":"autom8ter","contact":{"email":"coleman@autom8ter.com"}}
}

func ExampleDocument_Get() {

	doc := myjson.NewDocument()
	doc.Set("name", "autom8ter")
	doc.Set("contact.email", "coleman@autom8ter.com")

	fmt.Println(doc.String())
	// Output:
	// {"name":"autom8ter","contact":{"email":"coleman@autom8ter.com"}}
}
