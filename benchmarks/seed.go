package benchmarks

import (
	"context"

	"github.com/autom8ter/myjson"
	"github.com/autom8ter/myjson/kv"
	"github.com/autom8ter/myjson/testutil"
)

func seedDatabase(ctx context.Context, db myjson.Database) error {
	results, err := db.Query(ctx, "account", myjson.Q().Query())
	if err != nil {
		return err
	}
	if err := db.Tx(ctx, kv.TxOpts{}, func(ctx context.Context, tx myjson.Tx) error {
		for _, a := range results.Documents {
			for i := 0; i < 10; i++ {
				u := testutil.NewUserDoc()
				u.Set("account_id", a.Get("_id"))
				usrID, err := tx.Create(ctx, "user", u)
				if err != nil {
					return err
				}
				for i := 0; i < 3; i++ {
					t := testutil.NewTaskDoc(usrID)
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
