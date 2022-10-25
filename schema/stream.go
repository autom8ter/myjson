package schema

import "context"

// ChangeStreamHandler is a function executed on changes to documents which emit events
type ChangeStreamHandler func(ctx context.Context, change *StateChange) error
