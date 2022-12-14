package main

import (
	"context"
	_ "embed"
	"fmt"
	"os"

	"github.com/autom8ter/gokvkit"
	"github.com/autom8ter/gokvkit/httpapi"
	_ "github.com/autom8ter/gokvkit/kv/badger"
	"github.com/autom8ter/gokvkit/model"
	"github.com/autom8ter/gokvkit/testutil"
)

var (
	//go:embed schemas/task.yaml
	taskSchema string
	//go:embed schemas/user.yaml
	userSchema string
)

func main() {
	ctx := context.Background()
	os.MkdirAll("./tmp/crm", 0700)
	db, err := gokvkit.New(ctx, gokvkit.Config{
		KV: gokvkit.KVConfig{
			Provider: "badger",
			Params: map[string]any{
				"storage_path": "./tmp/crm",
			},
		},
	}, gokvkit.WithOnPersist(map[string][]gokvkit.OnPersist{
		"user": {
			{
				Name:   "cascade_delete_task",
				Before: true,
				Func:   cascadeDelete,
			},
		},
	}))
	if err != nil {
		panic(err)
	}
	if err := db.ConfigureCollection(ctx, []byte(userSchema)); err != nil {
		panic(err)
	}
	if err := db.ConfigureCollection(ctx, []byte(taskSchema)); err != nil {
		panic(err)
	}
	if err := setupDatabase(ctx, db); err != nil {
		panic(err)
	}
	o, err := httpapi.New(db, &httpapi.OpenAPIParams{
		Title:       "Example CRM API",
		Version:     "v0.0.0",
		Description: "an example crm api",
	})
	if err := o.Serve(context.Background(), 8080); err != nil {
		panic(err)
	}
}

func cascadeDelete(ctx context.Context, tx gokvkit.Tx, command *model.Command) error {
	if command.Action == model.Delete {
		results, err := tx.Query(ctx, "task", gokvkit.NewQueryBuilder().Where(model.Where{
			Field: "user",
			Op:    "==",
			Value: command.DocID,
		}).Query())
		if err != nil {
			return err
		}
		for _, result := range results.Documents {
			err = tx.Delete(ctx, "task", result.GetString("_id"))
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func setupDatabase(ctx context.Context, db *gokvkit.DB) error {
	fmt.Println("seeding database")
	for i := 0; i < 1000; i++ {
		if err := db.Tx(context.Background(), func(ctx context.Context, tx gokvkit.Tx) error {
			id, err := tx.Create(ctx, "user", testutil.NewUserDoc())
			if err != nil {
				return err
			}
			for i := 0; i < 5; i++ {
				if _, err := tx.Create(ctx, "task", testutil.NewTaskDoc(id)); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return err
		}
	}
	fmt.Println("finished seeding database")
	return nil
}
