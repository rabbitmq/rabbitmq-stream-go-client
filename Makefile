# Get the currently used golang install path (in GOPATH/bin, unless GOBIN is set)
ifeq (,$(shell go env GOBIN))
GOBIN=$(shell go env GOPATH)/bin
else
GOBIN=$(shell go env GOBIN)
endif

VERSION?=latest
LDFLAGS="-X main.Version=$(VERSION)"

all: vet

vet: $(go_sources)
	go vet ./pkg/...
	touch $@

build-perfTest: vet
	go build -ldflags=$(LDFLAGS) -o bin/perfTest perfTest/perftest.go

build: vet build-perfTest
	go build -ldflags=$(LDFLAGS) -v ./...

test: vet
	go test -v  ./pkg/streaming -race -coverprofile=coverage.txt -covermode=atomic
docker-build:
	docker build -t gsantomaggio/go-stream-client:$(VERSION)  .

docker-push: docker-build
	docker push gsantomaggio/go-stream-client:$(VERSION)

