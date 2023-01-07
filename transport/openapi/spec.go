package openapi

import (
	"net/http"

	"github.com/autom8ter/myjson"
)

func (o *openAPIServer) specHandler(db myjson.Database) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bits, _ := o.getSpec(r.Context(), db)
		w.WriteHeader(http.StatusOK)
		w.Write(bits)
	})
}
