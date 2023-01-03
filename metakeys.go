package myjson

type internalMetaKey string

const (
	internalKey   internalMetaKey = "_internal"
	isIndexingKey internalMetaKey = "_is_indexing"
	txCtx         internalMetaKey = "_tx_context"
)
