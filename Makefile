# Get the currently used golang install path (in GOPATH/bin, unless GOBIN is set)
ifeq (,$(shell go env GOBIN))
GOBIN = $(shell go env GOPATH)/bin
else
GOBIN = $(shell go env GOBIN)
endif

VERSION ?= latest
LDFLAGS = "-X main.Version=$(VERSION)"

all: test build


vet: $(go_sources)
	go vet ./pkg/...

fmt:
	go fmt ./...

STATICCHECK ?= $(GOBIN)/staticcheck
$(STATICCHECK):
	go get honnef.co/go/tools/cmd/staticcheck
check: $(STATICCHECK)
	$(STATICCHECK) ./pkg/streaming

test: vet fmt check
	go test -v  ./pkg/streaming -race -coverprofile=coverage.txt -covermode=atomic

integration-test: vet fmt check
	go test -v  ./pkg/system_integration -race -coverprofile=coverage.txt -covermode=atomic -tags debug

build: vet fmt check
	go build -ldflags=$(LDFLAGS) -v ./...

PERFTEST_FLAGS ?= silent -p 1 -c 1
perf-test-run: perf-test-build
	go run perfTest/perftest.go $(PERFTEST_FLAGS)

perf-test-build: vet fmt check
	go build -ldflags=$(LDFLAGS) -o bin/perfTest perfTest/perftest.go

perf-test-docker-build: perf-test-build
	docker build -t pivotalrabbitmq/go-stream-perf-test:$(VERSION) .

perf-test-docker-push: perf-test-docker-build
	docker push pivotalrabbitmq/go-stream-perf-test:$(VERSION)