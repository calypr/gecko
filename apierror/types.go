package apierror

type Type string

const (
	TypeUnauthorized                  Type = "unauthorized"
	TypeForbidden                     Type = "forbidden"
	TypeNotFound                      Type = "not_found"
	TypeMethodNotAllowed              Type = "method_not_allowed"
	TypeInvalidConfigType             Type = "invalid_config_type"
	TypeConfigNotFound                Type = "config_not_found"
	TypeInvalidJSON                   Type = "invalid_json"
	TypeEmptyRequestBody              Type = "empty_request_body"
	TypeInvalidRequestBody            Type = "invalid_request_body"
	TypeValidationFailed              Type = "validation_failed"
	TypeMissingAuthorization          Type = "missing_authorization"
	TypeInvalidAuthorizationResponse  Type = "invalid_authorization_response"
	TypeInvalidJWTHandler             Type = "invalid_jwt_handler"
	TypeInvalidProjectID              Type = "invalid_project_id"
	TypeMissingProjectID              Type = "missing_project_id"
	TypeProjectIDMismatch             Type = "project_id_mismatch"
	TypeInvalidDirectory              Type = "invalid_directory"
	TypeDatabaseError                 Type = "database_error"
	TypeDatabaseUnavailable           Type = "database_unavailable"
	TypeGraphQueryFailed              Type = "graph_query_failed"
	TypeInvalidDistance               Type = "invalid_distance"
	TypeInvalidVectorRequest          Type = "invalid_vector_request"
	TypeInvalidPointData              Type = "invalid_point_data"
	TypeMissingIdentifier             Type = "missing_identifier"
	TypeInvalidUUID                   Type = "invalid_uuid"
	TypeInvalidQueryParameter         Type = "invalid_query_parameter"
	TypePointNotFound                 Type = "point_not_found"
	TypeVectorCollectionNotFound      Type = "vector_collection_not_found"
	TypeVectorCollectionAlreadyExists Type = "vector_collection_already_exists"
	TypeVectorStoreUnavailable        Type = "vector_store_unavailable"
	TypeVectorOperationFailed         Type = "vector_operation_failed"
	TypeAuthorizationServiceError     Type = "authorization_service_error"
	TypeAppCardNotFound               Type = "app_card_not_found"
)

type Error struct {
	Type    Type           `json:"type"`
	Message string         `json:"message"`
	Code    int            `json:"code"`
	Details map[string]any `json:"details,omitempty"`
}
