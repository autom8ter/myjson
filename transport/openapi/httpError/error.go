package httpError

import (
	"encoding/json"
	"net/http"

	"github.com/autom8ter/myjson/errors"
)

func Error(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	var e = errors.Extract(err)
	if cde := e.Code; cde >= 400 && cde < 600 {
		status = int(cde)
	}
	w.WriteHeader(status)
	// remove the internal error
	json.NewEncoder(w).Encode(e)
	return
}
