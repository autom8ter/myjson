package openapi

import (
	"context"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/autom8ter/myjson"
	"github.com/autom8ter/myjson/errors"
	"github.com/autom8ter/myjson/extentions/openapi/httpError"
	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/spf13/cast"
)

// OpenAPIValidator validates inbound requests against the openapi schema
// adds openapi.path_params to the inbound metadata
// adds openapi.route to the inbound metadata
// adds openapi.header.${headerName} to the metadata
func (o *OpenAPIServer) openAPIValidator() mux.MiddlewareFunc {
	return func(handler http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			o.specMu.RLock()
			defer o.specMu.RUnlock()
			md, _ := myjson.GetMetadata(r.Context())
			if r.Header.Get("Authorization") != "" {
				md.Set("openapi.authorization", r.Header.Get("Authorization"))
			}
			route, pathParams, err := o.openapiRouter.FindRoute(r)
			if err != nil {
				httpError.Error(w, errors.Wrap(err, http.StatusNotFound, "route not found"))
				return
			}
			requestValidationInput := &openapi3filter.RequestValidationInput{
				Request:    r,
				PathParams: pathParams,
				Route:      route,
				Options: &openapi3filter.Options{AuthenticationFunc: func(ctx context.Context, input *openapi3filter.AuthenticationInput) error {
					//TODO: add auth
					return nil
				}},
			}
			md.SetAll(map[string]any{
				"openapi.path_params":  pathParams,
				"openapi.path":         route.Path,
				"openapi.operation_id": route.Operation.OperationID,
				"openapi.method":       route.Method,
			})
			if err := openapi3filter.ValidateRequest(r.Context(), requestValidationInput); err != nil {
				bits, _ := io.ReadAll(r.Body)
				o.logger.Error(r.Context(), "OPENAPI VALIDATION FAILURE", map[string]any{
					"error":                err,
					"openapi.request_body": string(bits),
				})
				httpError.Error(w, errors.Wrap(err, http.StatusBadRequest, "request failed validation"))
				return
			}

			handler.ServeHTTP(w, r.WithContext(md.ToContext(r.Context())))
		})
	}
}

func PathParams(r *http.Request) map[string]string {
	md, _ := myjson.GetMetadata(r.Context())
	params, ok := md.Get("openapi.path_params")
	if ok {
		return cast.ToStringMapString(params)
	}
	return map[string]string{}
}

func OperationID(r *http.Request) string {
	md, _ := myjson.GetMetadata(r.Context())
	params, ok := md.Get("openapi.operation_id")
	if ok {
		return cast.ToString(params)
	}
	return ""
}

func Method(r *http.Request) string {
	md, _ := myjson.GetMetadata(r.Context())
	params, ok := md.Get("openapi.method")
	if ok {
		return cast.ToString(params)
	}
	return ""
}

func Path(r *http.Request) string {
	md, _ := myjson.GetMetadata(r.Context())
	params, ok := md.Get("openapi.path")
	if ok {
		return cast.ToString(params)
	}
	return ""
}

func Authorization(r *http.Request) string {
	md, _ := myjson.GetMetadata(r.Context())
	params, ok := md.Get("openapi.authorization")
	if ok {
		return cast.ToString(params)
	}
	return ""
}

func (o *OpenAPIServer) loggerWare() mux.MiddlewareFunc {
	return func(handler http.Handler) http.Handler {
		return handlers.CustomLoggingHandler(os.Stdout, handler, func(writer io.Writer, params handlers.LogFormatterParams) {
			var fields = map[string]any{
				"size":        params.Size,
				"status_code": params.StatusCode,
				"elapsed":     time.Since(params.TimeStamp),
			}
			switch {
			case params.StatusCode >= 500:
				o.logger.Error(params.Request.Context(), "INTERNAL SERVER ERROR", fields)
			case params.StatusCode == 400:
				o.logger.Warn(params.Request.Context(), "BAD REQUEST", fields)

			case params.StatusCode == 401:
				o.logger.Warn(params.Request.Context(), "UNAUTHORIZED", fields)
			case params.StatusCode == 403:
				o.logger.Warn(params.Request.Context(), "FORBIDDEN", fields)
			case params.StatusCode == 404:
				o.logger.Warn(params.Request.Context(), "NOT FOUND", fields)
			case params.StatusCode == 200:
				o.logger.Info(params.Request.Context(), "OK", fields)
			default:
				o.logger.Info(params.Request.Context(), "REQUEST PROCESSED", fields)
			}
		})
	}
}
