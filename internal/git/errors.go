package git

import (
	"fmt"
	"net/http"
)

type ErrorKind string

const (
	ErrorKindValidation   ErrorKind = "validation"
	ErrorKindForbidden    ErrorKind = "forbidden"
	ErrorKindIntegration  ErrorKind = "integration"
	ErrorKindNotFound     ErrorKind = "not_found"
	ErrorKindDatabase     ErrorKind = "database"
	ErrorKindUnauthorized ErrorKind = "unauthorized"
)

type Error struct {
	Kind       ErrorKind
	Message    string
	StatusCode int
	Details    map[string]any
	Err        error
}

func (err *Error) Error() string {
	if err == nil {
		return ""
	}
	if err.Message != "" {
		return err.Message
	}
	if err.Err != nil {
		return err.Err.Error()
	}
	return http.StatusText(err.StatusCode)
}

func (err *Error) Unwrap() error {
	if err == nil {
		return nil
	}
	return err.Err
}

func NewError(kind ErrorKind, statusCode int, message string, details map[string]any) *Error {
	return &Error{
		Kind:       kind,
		StatusCode: statusCode,
		Message:    message,
		Details:    details,
	}
}

func WrapError(kind ErrorKind, statusCode int, message string, cause error, details map[string]any) *Error {
	if cause == nil {
		return NewError(kind, statusCode, message, details)
	}
	return &Error{
		Kind:       kind,
		StatusCode: statusCode,
		Message:    fmt.Sprintf("%s: %s", message, cause),
		Details:    details,
		Err:        cause,
	}
}
