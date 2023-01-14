package myjson

import "context"

type ctxKey int

const (
	metadataKey ctxKey = 0
)

var (
	// MetadataKeyNamespace is the key for the database namespace - it will return as "default" if not set
	MetadataKeyNamespace = "namespace"
	// MetadataKeyUserID is the key for the user id	for use in x-authorizers (optional)
	MetadataKeyUserID = "userId"
	// MetadataKeyRoles is the key for the user roles([]string)	for use in x-authorizers (optional)
	MetadataKeyRoles = "roles"
	// MetadataKeyGroups is the key for the user groups([]string) for use in x-authorizers (optional)
	MetadataKeyGroups = "groups"
)

// GetMetadataValue gets a metadata value from the context if it exists
func GetMetadataValue(ctx context.Context, key string) any {
	m, ok := ctx.Value(metadataKey).(*Document)
	if ok {
		val := m.Get(key)
		if val == nil && key == MetadataKeyNamespace {
			return "default"
		}
		return val
	}
	if key == MetadataKeyNamespace {
		return "default"
	}
	return nil
}

// SetMetadataValues sets metadata key value pairs in the context
func SetMetadataValues(ctx context.Context, data map[string]any) context.Context {
	m := ExtractMetadata(ctx)
	_ = m.SetAll(data)
	return context.WithValue(ctx, metadataKey, m)
}

// SetMetadataNamespace sets the metadata namespace
func SetMetadataNamespace(ctx context.Context, namespace string) context.Context {
	m := ExtractMetadata(ctx)
	_ = m.Set(MetadataKeyNamespace, namespace)
	return context.WithValue(ctx, metadataKey, m)
}

// SetMetadataUserID sets the metadata userID for targeting in the collections x-authorizers
func SetMetadataUserID(ctx context.Context, userID string) context.Context {
	m := ExtractMetadata(ctx)
	_ = m.Set(MetadataKeyUserID, userID)
	return context.WithValue(ctx, metadataKey, m)
}

// SetMetadataRoles sets the metadata user roles for targeting in the collections x-authorizers
func SetMetadataRoles(ctx context.Context, roles []string) context.Context {
	m := ExtractMetadata(ctx)
	_ = m.Set(MetadataKeyRoles, roles)
	return context.WithValue(ctx, metadataKey, m)
}

// SetMetadataGroups sets the metadata user groups for targeting in the collections x-authorizers
func SetMetadataGroups(ctx context.Context, groups []string) context.Context {
	m := ExtractMetadata(ctx)
	_ = m.Set(MetadataKeyGroups, groups)
	return context.WithValue(ctx, metadataKey, m)
}

// ExtractMetadata extracts metadata from the context and returns it
func ExtractMetadata(ctx context.Context) *Document {
	m, ok := ctx.Value(metadataKey).(*Document)
	if ok {
		return m
	}
	m = NewDocument()

	_ = m.Set(MetadataKeyNamespace, "default")
	return m
}
