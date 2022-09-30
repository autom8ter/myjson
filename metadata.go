package wolverine

import "context"

// Metadata are kv pairs associated with a database action
type MetaData map[string]string

// ToCtx injects the metadata into the context and returns a new context
// Metadata can then be extracted by the returned context with MetadataFromContext
func (m MetaData) ToCtx(ctx context.Context) context.Context {
	return context.WithValue(ctx, "wolverine.metadata", m)
}

// MetadataFromContext extracts the metadata from the context (if it exists)
func MetadataFromContext(ctx context.Context) (MetaData, bool) {
	val, ok := ctx.Value("wolverine.metadata").(MetaData)
	return val, ok
}
