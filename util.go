package wolverine

import (
	"sort"

	"github.com/tidwall/gjson"
)

func orderBy(orderBy OrderBy, documents []*Document) []*Document {
	if orderBy.Field == "" {
		return documents
	}
	if orderBy.Direction == DESC {
		sort.Slice(documents, func(i, j int) bool {
			return compareField(orderBy.Field, documents[i], documents[j])
		})
	} else {
		sort.Slice(documents, func(i, j int) bool {
			return !compareField(orderBy.Field, documents[i], documents[j])
		})
	}
	return documents
}

func compareField(field string, i, j *Document) bool {
	iFieldVal := i.result.Get(field)
	jFieldVal := j.result.Get(field)
	switch i.result.Get(field).Type {
	case gjson.Null:
		return false
	case gjson.False:
		return iFieldVal.Bool() && !jFieldVal.Bool()
	case gjson.Number:
		return iFieldVal.Float() > jFieldVal.Float()
	case gjson.String:
		return iFieldVal.String() > jFieldVal.String()
	default:
		return iFieldVal.String() > jFieldVal.String()
	}
}

func prunePage(page, limit int, documents []*Document) ([]*Document, bool) {
	if limit == 0 {
		return documents, false
	}
	startPage := (page * limit)

	if len(documents) <= startPage {
		return nil, false
	}
	if len(documents) <= startPage+limit {
		return documents[page*limit:], false
	} else {
		return documents[page*limit : startPage+limit], true
	}
}

func pipelineQuery(page, limit int, order OrderBy, input, output chan *Document, ordered bool) error {
	if ordered {
		return orderedPipeline(page, limit, input, output)
	}
	// if unordered, we must read full table of documents before sorting them
	var fullScan []*Document
	for doc := range input {
		fullScan = append(fullScan, doc)
	}
	if !ordered {
		fullScan = orderBy(order, fullScan)
		fullScan, _ = prunePage(page, limit, fullScan)
	}
	for _, doc := range fullScan {
		output <- doc
	}
	return nil
}

func orderedPipeline(page, limit int, input, output chan *Document) error {
	startOffset := page * limit
	endOffset := startOffset + limit
	received := 0
	for doc := range input {
		received++
		switch {
		case limit <= 0:
			output <- doc
		case limit >= 0:
			if received <= startOffset {
				continue
			}
			output <- doc
			//if len(*results) >= limit {
			//	return nil
			//}
			if received >= endOffset {
				return nil
			}
		}
	}
	return nil
}
