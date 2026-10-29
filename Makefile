.PHONY: format test run-example

format:
	go fmt ./...

test:
	go test ./... -race -coverprofile=coverage.txt -covermode=atomic -v

run-example:
	go run examples/main.go
