package httpError

import (
	"net/http"

	"github.com/palantir/stacktrace"
)

func Error(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	if cde := stacktrace.GetCode(err); cde >= 400 && cde < 600 {
		status = int(cde)
	}
	http.Error(w, stacktrace.RootCause(err).Error(), status)
	return
}
