package openapi

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/autom8ter/myjson"
	"github.com/autom8ter/myjson/errors"
	"github.com/autom8ter/myjson/extentions/openapi/httpError"
	"github.com/huandu/xstrings"
)

func (o *OpenAPIServer) getSDKHandler(db myjson.Database) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		pkgName := r.URL.Query().Get("pkg")
		if pkgName == "" {
			pkgName = xstrings.ToSnakeCase(fmt.Sprintf("%s_client", strings.TrimSpace(o.params.Title)))
		}
		if err := o.GenerateSDK(db, pkgName, w); err != nil {
			httpError.Error(w, errors.Wrap(err, 0, "failed to generate sdk"))
			return
		}
	})
}
