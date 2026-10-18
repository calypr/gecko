// Package apgen owns the OpenAPI document that defines the HTTP surface.
//
// The document under openapi/ is the single source of truth for the
// contract: the handlers under internal/ implement the operations it
// defines, and their request and response fields must match it.
//
// The request and response structs in internal/ are hand-written today;
// there is no generated code in this package yet, so keep the structs and
// the document in sync by hand when an operation changes.
package apgen

import "embed"

// SpecFS carries the OpenAPI document that defines the HTTP surface.
//
//go:embed openapi/gecko.yaml
var SpecFS embed.FS
