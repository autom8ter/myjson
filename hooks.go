package brutus

import "context"

// ValidatorHook is a hook function used to validate all new and updated documents being persisted to a collection
type ValidatorHook func(ctx context.Context, core CoreAPI, change *DocChange) error

// SideEffectHook is a hook function triggered whenever a document changes
type SideEffectHook func(ctx context.Context, core CoreAPI, change *DocChange) (*DocChange, error)

// WhereHook is a hook function triggered before queries/scans are executed. They may be used for a varietey of purposes (ex: query authorization hooks)
type WhereHook func(ctx context.Context, core CoreAPI, where []Where) ([]Where, error)
