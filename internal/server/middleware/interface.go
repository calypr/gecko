package middleware

type ResourceAccessHandler interface {
	GetAllowedResources(token, method, service string) ([]any, error)
	CheckResourceServiceAccess(token, method, service, resourcePath string) (bool, error)
}
