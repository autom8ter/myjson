package handlers

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/autom8ter/gokvkit"
	"github.com/autom8ter/gokvkit/httpapi/api"
	"github.com/autom8ter/gokvkit/httpapi/httpError"
	"github.com/autom8ter/gokvkit/model"
	"github.com/go-chi/chi/v5"
	"github.com/palantir/stacktrace"
)

func CreateDocHandler(o api.OpenAPIServer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		collection := chi.URLParam(r, "collection")
		if !o.DB().HasCollection(collection) {
			httpError.Error(w, stacktrace.NewErrorWithCode(http.StatusBadRequest, "collection does not exist"))
			return
		}
		var doc = model.NewDocument()
		if err := json.NewDecoder(r.Body).Decode(doc); err != nil {
			httpError.Error(w, stacktrace.PropagateWithCode(err, http.StatusBadRequest, "failed to decode query"))
			return
		}
		if err := o.DB().Tx(r.Context(), func(ctx context.Context, tx gokvkit.Tx) error {
			id, err := tx.Create(ctx, collection, doc)
			if err != nil {
				return stacktrace.Propagate(err, "")
			}
			if err := o.DB().SetPrimaryKey(collection, doc, id); err != nil {
				return stacktrace.Propagate(err, "")
			}
			return nil
		}); err != nil {
			httpError.Error(w, err)
			return
		}
		json.NewEncoder(w).Encode(doc)
	}
}
