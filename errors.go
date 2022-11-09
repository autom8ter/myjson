package brutus

import "github.com/palantir/stacktrace"

const (
	ErrEmptySchemaCollection stacktrace.ErrorCode = 4000
	ErrDocumentValidation    stacktrace.ErrorCode = 4001
	ErrSchemaLoad            stacktrace.ErrorCode = 4002
	ErrTODO                                       = 9000
)

// GetErrorCode returns the error code from the error
func GetErrorCode(err error) uint16 {
	return uint16(stacktrace.GetCode(err))
}

// RootCause returns the root cause of the error
func RootCause(err error) error {
	return stacktrace.RootCause(err)
}
