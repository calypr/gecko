.PHONY: _default clean swagger

_default: bin/gecko
	@:

# Simply depend on main.go, or leave the prerequisites blank. 
# Go handles the rest.
bin/gecko: main.go
	go build -o bin/gecko .

clean:
	rm -f bin/gecko

swagger: # help: generate swagger docs
	swag init --dir . --parseDependency --parseInternal --parseDepth 2