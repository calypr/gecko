package middleware

import "context"

type ResourceAccessHandler interface {
	GetAllowedResources(ctx context.Context, token, method, service string) ([]any, error)
	CheckResourceServiceAccess(ctx context.Context, token, method, service, resourcePath string) (bool, error)
}
