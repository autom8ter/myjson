package openapi

import (
	"net/http"
	"strings"

	"github.com/autom8ter/myjson/errors"
	"github.com/autom8ter/myjson/transport/openapi/httpError"
	"github.com/autom8ter/myjson/util"
)

func (o *openAPIServer) specHandler() http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, ".json") {
			bits, err := util.YAMLToJSON(o.spec)
			if err != nil {
				httpError.Error(w, errors.Wrap(err, errors.Internal, "failed to convert spec from yaml to json"))
				return
			}
			w.WriteHeader(http.StatusOK)
			w.Write(bits)
		} else {
			w.WriteHeader(http.StatusOK)
			w.Write(o.spec)
		}

	})
}
