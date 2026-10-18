package syfon

import (
	"errors"
	"fmt"
	"strings"
)

type HTTPError struct {
	Method string
	Path   string
	Status int
	Body   string
}

func (err HTTPError) Error() string {
	return fmt.Sprintf("syfon %s %s failed with status %d: %s", err.Method, err.Path, err.Status, strings.TrimSpace(err.Body))
}

func IsHTTPStatus(err error, statuses ...int) bool {
	var httpErr *HTTPError
	if !errors.As(err, &httpErr) {
		return false
	}
	for _, status := range statuses {
		if httpErr.Status == status {
			return true
		}
	}
	return false
}
