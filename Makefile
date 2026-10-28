OAPI_CODEGEN ?= go run github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen@v2.8.0
OPENAPI_FILE ?= apgen/openapi/gecko.yaml
SERVER_CONFIG ?= apgen/codegen/server.yaml
SERVER_OUT ?= apgen/server/gecko/gecko.gen.go

.PHONY: _default clean generate-openapi

_default: bin/gecko
	@:

# Simply depend on main.go, or leave the prerequisites blank.
# Go handles the rest.
bin/gecko: main.go
	go build -o bin/gecko .

clean:
	rm -f bin/gecko

generate-openapi: # help: regenerate strict server bindings from the OpenAPI spec
	@mkdir -p $(dir $(SERVER_OUT))
	@echo "Generating strict server bindings from $(OPENAPI_FILE)..."
	$(OAPI_CODEGEN) -config $(SERVER_CONFIG) $(OPENAPI_FILE) > $(SERVER_OUT)
	@echo "Wrote $(SERVER_OUT)"