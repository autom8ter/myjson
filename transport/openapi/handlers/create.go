package handlers

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/autom8ter/myjson"
	"github.com/autom8ter/myjson/errors"
	"github.com/autom8ter/myjson/kv"
	"github.com/autom8ter/myjson/transport/openapi/httpError"
	"github.com/gorilla/mux"
)

func CreateDocHandler(db myjson.Database) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		collection := mux.Vars(r)["collection"]
		if !db.HasCollection(r.Context(), collection) {
			httpError.Error(w, errors.New(errors.NotFound, "collection does not exist"))
			return
		}
		var doc = myjson.NewDocument()
		if err := json.NewDecoder(r.Body).Decode(doc); err != nil {
			httpError.Error(w, errors.Wrap(err, errors.Validation, "failed to decode query"))
			return
		}
		if err := db.Tx(r.Context(), kv.TxOpts{}, func(ctx context.Context, tx myjson.Tx) error {
			id, err := tx.Create(ctx, collection, doc)
			if err != nil {
				return err
			}
			if err := db.GetSchema(ctx, collection).SetPrimaryKey(doc, id); err != nil {
				return err
			}
			return nil
		}); err != nil {
			httpError.Error(w, err)
			return
		}
		json.NewEncoder(w).Encode(doc)
	}
}
