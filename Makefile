.PHONY: deps build

.DEFAULT_GOAL := build
REPO_PATH := github.com/leisurelyrcxf/redis-tools
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

redis-recovery:
	go build -ldflags "$(GO_LDFLAGS)" -a -tags "netgo osusergo" -installsuffix netgo -o redis-recovery ./cmd/recovery

redis-diff:
	go build -ldflags "$(GO_LDFLAGS)" -a -tags "netgo osusergo" -installsuffix netgo -o redis-diff ./cmd/diff

redis-find-empty:
	go build -ldflags "$(GO_LDFLAGS)" -a -tags "netgo osusergo" -installsuffix netgo -o redis-find-empty ./cmd/findempty

redis-flushdb:
	go build -ldflags "$(GO_LDFLAGS)" -a -tags "netgo osusergo" -installsuffix netgo -o redis-flushall ./cmd/flushdb

redis-find-key:
	go build -ldflags "$(GO_LDFLAGS)" -a -tags "netgo osusergo" -installsuffix netgo -o redis-find-key ./cmd/findkeys

redis-perf:
	go build -ldflags "$(GO_LDFLAGS)" -a -tags "netgo osusergo" -installsuffix netgo -o redis-perf ./cmd/perf

redis-analysis:
	go build -ldflags "$(GO_LDFLAGS)" -a -tags "netgo osusergo" -installsuffix netgo -o redis-analysis ./cmd/analysis

redis-fill:
	go build -ldflags "$(GO_LDFLAGS)" -a -tags "netgo osusergo" -installsuffix netgo -o redis-fill ./cmd/fill

binary: redis-recovery redis-diff redis-find-empty redis-flushdb redis-find-key redis-perf redis-analysis redis-fill

build: binary

recover: redis-recovery redis-diff

clean:
	rm -f redis-*

unit-test:
	go vet `go list ./... | grep -v '/vendor/' | grep -v '/tools'`
	go test -timeout 120m -count=1 -cover ./...

fmt:
	go list ./... | grep -v '/vendor/' | grep -v '/tools/' | xargs -I {} -n 1 find "${GOPATH}/src/{}/" -maxdepth 1 -iname "*.go" | xargs -n 1 goreturns -w -l

lint:
	golangci-lint run || true

fml: fmt lint
