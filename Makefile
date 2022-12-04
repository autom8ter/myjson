

setup:
	go install github.com/atombender/go-jsonschema/cmd/gojsonschema@latest


generate:
	gojsonschema -p model model/query.json > model/query.go
