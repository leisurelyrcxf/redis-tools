.PHONY: deps build binary

REPO_PATH := recovery
REVISION := $(shell git rev-parse HEAD || unknown)
BUILTAT := $(shell date +%Y-%m-%dT%H:%M:%S)
VERSION := $(shell git describe --tags $(shell git rev-list --tags --max-count=1))
GO_LDFLAGS ?= -s -X $(REPO_PATH)/versioninfo.REVISION=$(REVISION) \
			  -X $(REPO_PATH)/versioninfo.BUILTAT=$(BUILTAT) \
			  -X $(REPO_PATH)/versioninfo.VERSION=$(VERSION)
GO_MAJOR_VERSION = $(shell go version | cut -c 14- | cut -d' ' -f1 | cut -d'.' -f2)
MINIMUM_SUPPORTED_GO_MAJOR_VERSION = 13

deps:
	echo "GO_MAJOR_VERSION: $(GO_MAJOR_VERSION)"
	@if [ $(GO_MAJOR_VERSION) -ge $(MINIMUM_SUPPORTED_GO_MAJOR_VERSION) ]; then \
		echo "Use go env -w to set GONOSUMDB, GONOPROXY, GOPRIVATE"; \
		go env -w GONOSUMDB="git.garena.com"; \
		go env -w GONOPROXY="git.garena.com"; \
		go env -w GOPRIVATE="git.garena.com"; \
	else \
		echo "Use export env var to set GONOSUMDB, GONOPROXY, GOPRIVATE"; \
		export GO111MODULE=on; \
		export GONOSUMDB="git.garena.com"; \
		export GONOPROXY="git.garena.com"; \
		export GOPRIVATE="git.garena.com"; \
	fi
	go env
	env GO111MODULE=on go mod download
	env GO111MODULE=on go mod vendor

recovery:
	go build -ldflags "$(GO_LDFLAGS)" -a -tags "netgo osusergo" -installsuffix netgo -o recovery ./cmd/recovery

cleanup:
	go build -ldflags "$(GO_LDFLAGS)" -a -tags "netgo osusergo" -installsuffix netgo -o cleanup ./cmd/cleanup

flushdb:
	go build -ldflags "$(GO_LDFLAGS)" -a -tags "netgo osusergo" -installsuffix netgo -o flushall ./cmd/flushdb

find-key:
	go build -ldflags "$(GO_LDFLAGS)" -a -tags "netgo osusergo" -installsuffix netgo -o find-key ./cmd/findkeys

perf:
	go build -ldflags "$(GO_LDFLAGS)" -a -tags "netgo osusergo" -installsuffix netgo -o hperf ./cmd/perf

analysis:
	go build -ldflags "$(GO_LDFLAGS)" -a -tags "netgo osusergo" -installsuffix netgo -o analysis ./cmd/analysis

binary: recovery flushdb cleanup analysis

build: deps binary

unit-test:
	go vet `go list ./... | grep -v '/vendor/' | grep -v '/tools'`
	go test -timeout 120m -count=1 -cover ./...

fmt:
	go list ./... | grep -v '/vendor/' | grep -v '/tools/' | xargs -I {} -n 1 find "${GOPATH}/src/{}/" -maxdepth 1 -iname "*.go" | xargs -n 1 goreturns -w -l

lint:
	golangci-lint run || true

fml: fmt lint
