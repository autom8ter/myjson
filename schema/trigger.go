package schema

import (
	"context"
)

type Timing string

const (
	Before Timing = "before"
	After  Timing = "after"
)

type Hooks struct {
	QueryHooks       []QueryHook       `json:"queryHooks"`
	SearchQueryHooks []SearchQueryHook `json:"searchQueryHooks"`
	GetHooks         []GetHook         `json:"getHooks"`
	ReadHooks        []ReadHook        `json:"readHooks"`
	StateChangeHooks []StateChangeHook `json:"stateChangeHooks"`
}

type QueryHook struct {
	Name            string                                                                 `json:"name"`
	Function        func(ctx context.Context, query Query) (context.Context, Query, error) `json:"-"`
	IsInternal      bool                                                                   `json:"isInternal"`
	IsDeterministic bool                                                                   `json:"isDeterministic"`
}

type SearchQueryHook struct {
	Name            string                                                                             `json:"name"`
	Timing          Timing                                                                             `json:"timing"`
	Function        func(ctx context.Context, query SearchQuery) (context.Context, SearchQuery, error) `json:"-"`
	IsInternal      bool                                                                               `json:"isInternal"`
	IsDeterministic bool                                                                               `json:"isDeterministic"`
}

type AggregateQueryHook struct {
	Name            string                                                                                   `json:"name"`
	Timing          Timing                                                                                   `json:"timing"`
	Function        func(ctx context.Context, query AggregateQuery) (context.Context, AggregateQuery, error) `json:"-"`
	IsInternal      bool                                                                                     `json:"isInternal"`
	IsDeterministic bool                                                                                     `json:"isDeterministic"`
}

type GetHook struct {
	Name            string                                     `json:"name"`
	Timing          Timing                                     `json:"timing"`
	Function        func(ctx context.Context, id string) error `json:"-"`
	IsInternal      bool                                       `json:"isInternal"`
	IsDeterministic bool                                       `json:"isDeterministic"`
}

type ReadHook struct {
	Name            string                                                                          `json:"name"`
	Timing          Timing                                                                          `json:"timing"`
	Function        func(ctx context.Context, document Document) (context.Context, Document, error) `json:"-"`
	IsInternal      bool                                                                            `json:"isInternal"`
	IsDeterministic bool                                                                            `json:"isDeterministic"`
}

type StateChangeHook struct {
	Name            string                                                                              `json:"name"`
	Timing          Timing                                                                              `json:"timing"`
	Function        func(ctx context.Context, change StateChange) (context.Context, StateChange, error) `json:"-"`
	IsInternal      bool                                                                                `json:"isInternal"`
	IsDeterministic bool                                                                                `json:"isDeterministic"`
}
