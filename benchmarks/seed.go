package benchmarks

import (
	"context"

	"github.com/autom8ter/myjson"
	"github.com/autom8ter/myjson/kv"
	"github.com/autom8ter/myjson/testutil"
)

//nolint:deadcode,unused
func seedDatabase(ctx context.Context, db myjson.Database) error {
	results, err := db.Query(ctx, "account", myjson.Q().Query())
	if err != nil {
		return err
	}
	if err := db.Tx(ctx, kv.TxOpts{IsBatch: true}, func(ctx context.Context, tx myjson.Tx) error {
		for _, a := range results.Documents {
			for i := 0; i < 10; i++ {
				u := testutil.NewUserDoc()
				if err := u.Set("account_id", a.Get("_id")); err != nil {
					return err
				}
				if err := tx.Set(ctx, "user", u); err != nil {
					return err
				}

				for i := 0; i < 3; i++ {
					t := testutil.NewTaskDoc(u.GetString("_id"))
					_, err := tx.Create(ctx, "task", t)
					if err != nil {
						return err
					}
				}
			}
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}
