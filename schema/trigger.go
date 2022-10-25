package schema

import (
	"context"
)

type Timing string

const (
	Before Timing = "before"
	After  Timing = "after"
)

// Trigger are executed on documents before they are written to the database
type Trigger func(ctx context.Context, action Action, timing Timing, before, after Document) error
