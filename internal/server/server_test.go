package server

import (
	"io"
	"log"
	"testing"
)

func TestMakeRouterConfiguresLargeRequestBodyLimit(t *testing.T) {
	app := NewServer().WithLogger(log.New(io.Discard, "", 0)).MakeRouter()

	const wantBodyLimit = 50 * 1024 * 1024
	if got := app.Config().BodyLimit; got != wantBodyLimit {
		t.Fatalf("BodyLimit = %d, want %d", got, wantBodyLimit)
	}
}
