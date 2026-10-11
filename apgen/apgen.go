package apgen

// Package apgen is the API-first entry point for the HTTP surface.
//
// It follows the same layout as calypr/turbine:
//   - the OpenAPI document under openapi/ is the single source of truth
//   - the request/response types in this package are generated from it
//   - the domain services under internal/ implement the generated interface
//
// Do not edit oapi_gen.go by hand; edit openapi/gecko.yaml and re-run:
//
//	go generate ./apgen/...
package apgen

//go:embed openapi/gecko.yaml
var SpecFS embed.FS
