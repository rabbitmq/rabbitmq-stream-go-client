# Get the currently used golang install path (in GOPATH/bin, unless GOBIN is set)
ifeq (,$(shell go env GOBIN))
GOBIN=$(shell go env GOPATH)/bin
else
GOBIN=$(shell go env GOBIN)
endif

VERSION?=latest
LDFLAGS="-X main.Version=$(VERSION)"

PERFTEST_FLAGS?= silent -p 1 -c 1

all: vet

check:
	staticcheck pkg/streaming/*.go

vet: $(go_sources)
	go vet ./pkg/...
	touch $@

fmt:
	go fmt ./...

build-perfTest: vet
	go build -ldflags=$(LDFLAGS) -o bin/perfTest perfTest/perftest.go

build: vet build-perfTest
	go build -ldflags=$(LDFLAGS) -v ./...

test: vet
	go test -v  ./pkg/streaming -race -coverprofile=coverage.txt -covermode=atomic
docker-build: build
	docker build -t pivotalrabbitmq/go-stream-perf-test:$(VERSION)  .

docker-push: docker-build
	docker push pivotalrabbitmq/go-stream-perf-test:$(VERSION)

run-perfTest: build-perfTest
	go run perfTest/perftest.go $(PERFTEST_FLAGS)
