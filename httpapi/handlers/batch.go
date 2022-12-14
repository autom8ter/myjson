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

func BatchSetHandler(o api.OpenAPIServer) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		collection := chi.URLParam(r, "collection")
		if !o.DB().HasCollection(collection) {
			httpError.Error(w, stacktrace.NewErrorWithCode(http.StatusBadRequest, "collection does not exist"))
			return
		}
		var docs []*model.Document
		if err := json.NewDecoder(r.Body).Decode(&docs); err != nil {
			httpError.Error(w, stacktrace.PropagateWithCode(err, http.StatusBadRequest, "failed to decode query"))
			return
		}
		if err := o.DB().Tx(r.Context(), func(ctx context.Context, tx gokvkit.Tx) error {
			for _, d := range docs {
				if err := tx.Set(ctx, collection, d); err != nil {
					return stacktrace.Propagate(err, "")
				}
			}
			return nil
		}); err != nil {
			httpError.Error(w, err)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
}
