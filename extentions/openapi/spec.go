package openapi

import (
	"net/http"
	"strings"

	"github.com/autom8ter/myjson/errors"
	"github.com/autom8ter/myjson/extentions/openapi/httpError"
	"github.com/autom8ter/myjson/util"
)

func (o *OpenAPIServer) specHandler() http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, ".json") {
			w.Header().Set("Content-Type", "application/json")
			bits, err := util.YAMLToJSON(o.spec)
			if err != nil {
				httpError.Error(w, errors.Wrap(err, errors.Internal, "failed to convert spec from yaml to json"))
				return
			}
			w.WriteHeader(http.StatusOK)
			w.Write(bits)
		} else {
			w.Header().Set("Content-Type", "application/x-yaml")
			w.WriteHeader(http.StatusOK)
			w.Write(o.spec)
		}

	})
}
