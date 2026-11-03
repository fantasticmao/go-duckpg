VERSION=$(shell cat VERSION)

.PHONY: get format test run-example release

get:
	go get -u -t ./...

format:
	go fmt ./...

test:
	go test ./... -race -coverprofile=coverage.txt -covermode=atomic -v

run-example:
	go run ./examples

run-benchmark:
	go run ./benchmarks

release:
	git tag "$(VERSION)"
