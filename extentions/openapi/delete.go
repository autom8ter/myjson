package openapi

import (
	"context"
	"net/http"

	"github.com/autom8ter/myjson"
	"github.com/autom8ter/myjson/errors"
	"github.com/autom8ter/myjson/extentions/openapi/httpError"
	"github.com/autom8ter/myjson/kv"
	"github.com/gorilla/mux"
)

func (o *OpenAPIServer) deleteDocHandler(db myjson.Database) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		collection := mux.Vars(r)["collection"]
		if !db.HasCollection(r.Context(), collection) {
			httpError.Error(w, errors.New(errors.Validation, "collection does not exist"))
			return
		}
		docID := mux.Vars(r)["docID"]
		if err := db.Tx(r.Context(), kv.TxOpts{}, func(ctx context.Context, tx myjson.Tx) error {
			err := tx.Delete(ctx, collection, docID)
			if err != nil {
				return err
			}
			return nil
		}); err != nil {
			httpError.Error(w, err)
			return
		}
		w.WriteHeader(http.StatusOK)
	}
}
