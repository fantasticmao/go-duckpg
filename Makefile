VERSION=$(shell cat VERSION)

.PHONY: get format test benchmark run-example release

get:
	go get -u -t ./...

format:
	go fmt ./...

test:
	go test ./duckpg/... -race -coverpkg=./duckpg/... -coverprofile=coverage.txt -covermode=atomic -v

benchmark:
	go test -bench=. -count=1 ./benchmarks

run-example:
	go run ./examples

release:
	git tag "$(VERSION)"
