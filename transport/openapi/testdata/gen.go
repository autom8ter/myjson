//go:generate oapi-codegen -generate types -o types.go -package testdata openapi.yaml
//go:generate oapi-codegen -generate spec -o spec.go -package testdata openapi.yaml
//go:generate oapi-codegen -generate client -o client.go -package testdata openapi.yaml

package testdata
