.PHONY: test
build : build/assaf-server
build/assaf-server: cmd/assaf-http/main.go
	go build -ldflags -installsuffix cgo '-extldflags "-static"' -o ./build/assaf-server cmd/assaf-http/main.go

test: 
	go test ./...
