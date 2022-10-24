package schema

import (
	"container/list"
	"context"
	"encoding/json"
	"github.com/autom8ter/wolverine/errors"
	"github.com/palantir/stacktrace"
	"github.com/spf13/cast"
	"reflect"
	"strings"
	"sync"
)

type AggregateIndex struct {
	mu         *sync.RWMutex
	GroupBy    []string    `json:"groupBy"`
	Aggregates []Aggregate `json:"aggregates"`
	metrics    map[string]map[Aggregate]*list.List
}

func (a *AggregateIndex) UnmarshalJSON(bytes []byte) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.mu == nil {
		a.mu = &sync.RWMutex{}
	}
	a.metrics = map[string]map[Aggregate]*list.List{}
	return stacktrace.PropagateWithCode(json.Unmarshal(bytes, a), errors.ErrTODO, "")

}

func (a *AggregateIndex) Matches(query AggregateQuery) bool {
	if strings.Join(query.GroupBy, ",") != strings.Join(a.GroupBy, ",") {
		return false
	}
	for _, agg := range query.Aggregates {
		hasMatch := false
		for _, agg2 := range a.Aggregates {
			if reflect.DeepEqual(agg, agg2) {
				hasMatch = true
			}
		}
		if !hasMatch {
			return false
		}
	}
	return true
}

func (a *AggregateIndex) Aggregate(Aggregates ...Aggregate) []*Document {
	a.mu.RLock()
	defer a.mu.RUnlock()
	var documents []*Document
	for k, aggs := range a.metrics {
		d := NewDocument()
		splitValues := strings.Split(k, ".")
		for i, group := range a.GroupBy {
			d.Set(group, splitValues[i])
		}
		for agg, metric := range aggs {
			for _, aggregate := range Aggregates {
				if reflect.DeepEqual(agg, aggregate) {
					d.Set(agg.Alias, cast.ToFloat64(metric.Front().Value))
				}
			}
		}
		documents = append(documents, d)
	}
	return documents
}

func (a *AggregateIndex) Trigger() Trigger {
	return func(ctx context.Context, action Action, timing Timing, before, after *Document) error {
		a.mu.Lock()
		defer a.mu.Unlock()
		switch action {
		case Delete:
			var groupValues []string
			for _, g := range a.GroupBy {
				groupValues = append(groupValues, cast.ToString(before.Get(g)))
			}
			groupKey := strings.Join(groupValues, ".")
			if a.metrics[groupKey] == nil {
				a.metrics[groupKey] = map[Aggregate]*list.List{}
			}
			group := a.metrics[groupKey]
			for _, agg := range a.Aggregates {
				if group[agg] == nil {
					group[agg] = list.New()
				}
				group[agg].MoveToBack(group[agg].Front())
				if group[agg].Len() > 2 {
					for i := 0; i < group[agg].Len(); i++ {
						element := group[agg].Front().Next()
						if element != nil && i > 2 {
							group[agg].Remove(element)
						}
					}
				}
			}
		default:
			var groupValues []string
			for _, g := range a.GroupBy {
				groupValues = append(groupValues, cast.ToString(after.Get(g)))
			}
			groupKey := strings.Join(groupValues, ".")
			if a.metrics[groupKey] == nil {
				a.metrics[groupKey] = map[Aggregate]*list.List{}
			}
			group := a.metrics[groupKey]
			for _, agg := range a.Aggregates {
				if group[agg] == nil {
					group[agg] = list.New()
				}
				current := group[agg].Front()

				switch agg.Function {
				case SUM:
					value := after.GetFloat(agg.Field)
					if current == nil {
						group[agg].PushFront(value)
					} else {
						group[agg].PushFront(cast.ToFloat64(current.Value) + value)
					}

				case COUNT:
					if current == nil {
						group[agg].PushFront(1)
					} else {
						group[agg].PushFront(cast.ToFloat64(current.Value) + 1)
					}
				case MAX:
					value := after.GetFloat(agg.Field)
					if current == nil {
						group[agg].PushFront(value)
					} else {
						if value > cast.ToFloat64(current.Value) {
							group[agg].PushFront(value)
						}
					}
				case MIN:
					value := after.GetFloat(agg.Field)
					if current == nil {
						group[agg].PushFront(value)
					} else {
						if value < cast.ToFloat64(current.Value) {
							group[agg].PushFront(value)
						}
					}
				default:
					return stacktrace.NewError("unsupported aggregate function: %s", agg.Function)
				}
			}
		}
		return nil
	}
}
