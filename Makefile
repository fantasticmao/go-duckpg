.PHONY: format test

format:
	go fmt ./...

test:
	go test ./... -race -coverprofile=coverage.txt -covermode=atomic -v
