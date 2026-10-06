package apierror

type Type string

const (
	TypeUnauthorized                 Type = "unauthorized"
	TypeForbidden                    Type = "forbidden"
	TypeNotFound                     Type = "not_found"
	TypeMethodNotAllowed             Type = "method_not_allowed"
	TypeInvalidConfigType            Type = "invalid_config_type"
	TypeConfigNotFound               Type = "config_not_found"
	TypeInvalidJSON                  Type = "invalid_json"
	TypeEmptyRequestBody             Type = "empty_request_body"
	TypeInvalidRequestBody           Type = "invalid_request_body"
	TypeValidationFailed             Type = "validation_failed"
	TypeMissingAuthorization         Type = "missing_authorization"
	TypeInvalidAuthorizationResponse Type = "invalid_authorization_response"
	TypeInvalidJWTHandler            Type = "invalid_jwt_handler"
	TypeInvalidProjectID             Type = "invalid_project_id"
	TypeMissingProjectID             Type = "missing_project_id"
	TypeProjectIDMismatch            Type = "project_id_mismatch"
	TypeInvalidDirectory             Type = "invalid_directory"
	TypeDatabaseError                Type = "database_error"
	TypeDatabaseUnavailable          Type = "database_unavailable"
	TypeGraphQueryFailed             Type = "graph_query_failed"
	TypeInvalidQueryParameter        Type = "invalid_query_parameter"
	TypeAuthorizationServiceError    Type = "authorization_service_error"
	TypeAppCardNotFound              Type = "app_card_not_found"
)

type Error struct {
	Type    Type           `json:"type"`
	Message string         `json:"message"`
	Code    int            `json:"code"`
	Details map[string]any `json:"details,omitempty"`
}
