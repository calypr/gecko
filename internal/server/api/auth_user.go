package api

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/calypr/gecko/apierror"
	"github.com/calypr/gecko/internal/git"
	"github.com/calypr/gecko/internal/httputil"
	"github.com/gofiber/fiber/v3"
)

func (handler *Handler) authenticatedUserID(ctx fiber.Ctx) (string, *httputil.ErrorResponse) {
	authorizationHeader, tokenErr := git.ValidateAuthorizationHeader(ctx.Get("Authorization"))
	if tokenErr != nil {
		return "", httputil.NewError(apierror.TypeMissingAuthorization, tokenErr.Error(), http.StatusUnauthorized, nil, nil)
	}
	if handler.jwtApp == nil {
		return "", httputil.NewError(apierror.TypeInvalidJWTHandler, "JWT validation is not configured", http.StatusUnauthorized, nil, nil)
	}
	claims, err := handler.jwtApp.Decode(git.CleanAccessToken(authorizationHeader))
	if err != nil {
		return "", httputil.NewError(apierror.TypeUnauthorized, fmt.Sprintf("failed to decode authorization token: %s", err), http.StatusUnauthorized, nil, nil)
	}
	for _, claim := range []string{"sub", "username", "email"} {
		if value, ok := (*claims)[claim].(string); ok && strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value), nil
		}
	}
	return "", httputil.NewError(apierror.TypeUnauthorized, "authorization token does not include a stable user id", http.StatusUnauthorized, nil, nil)
}
