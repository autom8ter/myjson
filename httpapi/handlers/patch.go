package handlers

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/autom8ter/gokvkit"
	"github.com/autom8ter/gokvkit/httpapi/api"
	"github.com/autom8ter/gokvkit/httpapi/httpError"
	"github.com/go-chi/chi/v5"
	"github.com/palantir/stacktrace"
)

func PatchDocHandler(o api.OpenAPIServer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		collection := chi.URLParam(r, "collection")
		if !o.DB().HasCollection(collection) {
			httpError.Error(w, stacktrace.NewErrorWithCode(http.StatusBadRequest, "collection does not exist"))
			return
		}
		docID := chi.URLParam(r, "docID")
		var update = map[string]any{}
		if err := json.NewDecoder(r.Body).Decode(&update); err != nil {
			httpError.Error(w, stacktrace.PropagateWithCode(err, http.StatusBadRequest, "failed to decode edit"))
			return
		}
		if err := o.DB().Tx(r.Context(), func(ctx context.Context, tx gokvkit.Tx) error {
			err := tx.Update(ctx, collection, docID, update)
			if err != nil {
				return stacktrace.Propagate(err, "")
			}
			return nil
		}); err != nil {
			httpError.Error(w, err)
			return
		}
		doc, err := o.DB().Get(r.Context(), collection, docID)
		if err != nil {
			httpError.Error(w, stacktrace.PropagateWithCode(err, http.StatusBadRequest, "failed to edit document"))
			return
		}
		json.NewEncoder(w).Encode(doc)
	}
}
