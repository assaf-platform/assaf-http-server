.PHONY: test
build : build/assaf-server
build/assaf-server: cmd/assaf-http/main.go
	go build -o ./build/assaf-server cmd/assaf-http/main.go 

test: 
	go test ./...
