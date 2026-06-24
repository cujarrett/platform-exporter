default:
    @just --list

ci: lint test build

lint:
    go mod tidy -diff
    golangci-lint run

test:
    go test -race ./...

build:
    go build -o platform-exporter .

run:
    go run .
