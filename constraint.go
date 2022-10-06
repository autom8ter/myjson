package wolverine

import "context"

type Constraint func(ctx context.Context, event *Event) (bool, error)
