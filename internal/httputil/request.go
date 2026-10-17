package httputil

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"

	"github.com/calypr/gecko/apierror"
)

func ParseJSONBody(body []byte, x any, details map[string]any) *ErrorResponse {
	if !json.Valid(body) {
		return NewError(apierror.TypeInvalidJSON, "Invalid JSON format", http.StatusBadRequest, details, nil)
	}
	if errResponse := unmarshal(body, x); errResponse != nil {
		errResponse.Error.Details = MergeErrorDetails(errResponse.Error.Details, details)
		return errResponse
	}
	return nil
}

func unmarshal(body []byte, x any) *ErrorResponse {
	if len(body) == 0 {
		return NewError(apierror.TypeEmptyRequestBody, "empty request body", http.StatusBadRequest, nil, nil)
	}

	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	err := dec.Decode(x)
	if err != nil {
		structType := reflect.TypeOf(x)
		if structType.Kind() == reflect.Ptr {
			structType = structType.Elem()
		}

		msg := fmt.Sprintf("could not parse %s from JSON; make sure input has correct types", structType)
		// Deliberately excludes the request body: callers already have it, and
		// echoing it back would surface credentials and personal data in
		// client-visible error details (and in anything that logs these responses).
		errDetails := map[string]any{
			"target_type": structType.String(),
		}

		switch e := err.(type) {
		case *json.SyntaxError:
			msg = "Malformed JSON syntax"
			errDetails["offset"] = e.Offset
			return NewError(apierror.TypeInvalidJSON, msg, http.StatusBadRequest, errDetails, &err)
		case *json.UnmarshalTypeError:
			msg = fmt.Sprintf("Invalid type for field %q; expected %s", e.Field, e.Type)
			errDetails["field"] = e.Field
			errDetails["expected_type"] = e.Type.String()
			return NewError(apierror.TypeInvalidRequestBody, msg, http.StatusBadRequest, errDetails, &err)
		default:
			return NewError(apierror.TypeInvalidRequestBody, msg, http.StatusBadRequest, errDetails, &err)
		}
	}
	return nil
}

func MergeErrorDetails(base map[string]any, extra map[string]any) map[string]any {
	if len(extra) == 0 {
		return base
	}
	if base == nil {
		base = map[string]any{}
	}
	for k, v := range extra {
		base[k] = v
	}
	return base
}
