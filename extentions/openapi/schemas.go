package openapi

import (
	"encoding/json"
	"io"
	"net/http"

	"github.com/autom8ter/myjson"
	"github.com/autom8ter/myjson/errors"
	"github.com/autom8ter/myjson/extentions/openapi/httpError"
	"github.com/gorilla/mux"
)

func (o *OpenAPIServer) getSchemasHandler(db myjson.Database) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		bits, _ := json.Marshal(db.Collections(r.Context()))
		w.WriteHeader(http.StatusOK)
		w.Write(bits)
	})
}

func (o *OpenAPIServer) getSchemaHandler(db myjson.Database) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-yaml")
		collection := mux.Vars(r)["collection"]
		if !db.HasCollection(r.Context(), collection) {
			httpError.Error(w, errors.New(errors.Validation, "collection does not exist"))
			return
		}
		schema, err := db.GetSchema(r.Context(), collection).MarshalYAML()
		if err != nil {
			httpError.Error(w, errors.Wrap(err, http.StatusInternalServerError, "failed to marshal schema"))
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write(schema)
	})
}

func (o *OpenAPIServer) putSchemaHandler(db myjson.Database) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-yaml")
		bits, err := io.ReadAll(r.Body)
		if err != nil {
			httpError.Error(w, errors.Wrap(err, http.StatusBadRequest, "failed to read request body"))
			return
		}
		if err := db.ConfigureCollection(r.Context(), bits); err != nil {
			httpError.Error(w, errors.Wrap(err, http.StatusBadRequest, "failed to configure collection"))
			return
		}
		w.WriteHeader(http.StatusOK)
	})
}
