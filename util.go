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
