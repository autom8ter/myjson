package main

import (
	"context"
	"net/http"

	wolverinehttp "github.com/autom8ter/wolverine/transport/http"

	"github.com/autom8ter/wolverine"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	db, err := wolverine.New(ctx, wolverine.Config{
		Path:  "task.db",
		Debug: true,

		Collections: []wolverine.Collection{
			{
				Name: "task",

				Indexes: []wolverine.Index{
					{
						Fields: []string{"owner"},
					},
					{
						Fields: []string{"account_id", "done"},
					},
					{
						Fields:   []string{"*"},
						FullText: true,
					},
				},
			},
		},
		Migrations: []wolverine.Migration{
			{
				Name: "seed_task",
				Function: func(ctx context.Context, db wolverine.DB) error {
					var tasks []*wolverine.Document
					for i := 0; i < 1000; i++ {
						tasks = append(tasks, randomTask())
					}
					if err := db.BatchSet(ctx, "task", tasks); err != nil {
						return err
					}
					return nil
				},
			},
		},
	})
	if err != nil {
		db.Error(ctx, "server failure", err, map[string]interface{}{})
		return
	}
	defer db.Close(ctx)

	handler, err := wolverinehttp.Handler(db)
	if err != nil {
		db.Error(ctx, "failed to setup handler", err, map[string]interface{}{})
		return
	}
	addr := ":8080"
	db.Info(ctx, "starting server", map[string]interface{}{"addr": addr})
	if err := http.ListenAndServe(addr, handler); err != nil {
		db.Error(ctx, "server failure", err, map[string]interface{}{"addr": addr})
	}
}
