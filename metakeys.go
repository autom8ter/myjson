package myjson

import (
	"context"
)

type internalMetaKey string

const (
	internalKey   internalMetaKey = "_internal"
	isIndexingKey internalMetaKey = "_is_indexing"
)

func isInternal(ctx context.Context) bool {
	return ctx.Value(internalKey) == true
}

func isIndexing(ctx context.Context) bool {
	return ctx.Value(isIndexingKey) == true
}

// SetIsInternal sets a context value to indicate that the request is internal (it should only be used to bypass things like authorization, validation, etc)
func SetIsInternal(ctx context.Context) context.Context {
	return context.WithValue(ctx, internalKey, true)
}
