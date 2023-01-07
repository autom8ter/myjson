package openapi

import (
	"encoding/json"
	"net/http"

	"github.com/autom8ter/myjson"
	"github.com/autom8ter/myjson/errors"
	"github.com/autom8ter/myjson/transport/openapi/httpError"
	"github.com/gorilla/mux"
)

func (o *openAPIServer) getDocHandler(db myjson.Database) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		collection := mux.Vars(r)["collection"]
		if !db.HasCollection(r.Context(), collection) {
			httpError.Error(w, errors.New(errors.Validation, "collection does not exist"))
			return
		}
		docID := mux.Vars(r)["docID"]
		doc, err := db.Get(r.Context(), collection, docID)
		if err != nil {
			httpError.Error(w, errors.Wrap(err, http.StatusNotFound, "failed to get document"))
			return
		}
		json.NewEncoder(w).Encode(doc)
	}
}
