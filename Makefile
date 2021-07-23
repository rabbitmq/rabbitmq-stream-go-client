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
	go vet ./pkg/stream

fmt:
	go fmt ./...

STATICCHECK ?= $(GOBIN)/staticcheck
$(STATICCHECK):
	go get honnef.co/go/tools/cmd/staticcheck
check: $(STATICCHECK)
	$(STATICCHECK) ./pkg/stream

test: vet fmt check
	go test --tags=debug -v  ./pkg/stream -coverprofile=coverage.txt -covermode=atomic  #-ginkgo.v

build-all: vet fmt check build-darwin build-windows build-linux
	 go test --tags=debug -v -race ./pkg/stream -coverprofile=coverage.txt -covermode=atomic  #-ginkgo.v

integration-test: vet fmt check
	cd ./pkg/system_integration && go test -v  . -race -coverprofile=coverage.txt -covermode=atomic -tags debug -timeout 99999s

build-%: vet fmt check
	GOOS=$(*) GOARCH=amd64 go build -ldflags=$(LDFLAGS) -v ./...

build: vet fmt check
	go build -ldflags=$(LDFLAGS) -v ./...

PERFTEST_FLAGS ?= --publishers 1 --consumers 1
perf-test-run: perf-test-build
	go run perfTest/perftest.go silent $(PERFTEST_FLAGS)

perf-test-help: perf-test-build
	go run perfTest/perftest.go help

perf-test-build: vet fmt check
	go build -ldflags=$(LDFLAGS) -o bin/perfTest perfTest/perftest.go

perf-test-docker-build: perf-test-build
	docker build -t pivotalrabbitmq/go-stream-perf-test:$(VERSION) .

perf-test-docker-push: perf-test-docker-build
	docker push pivotalrabbitmq/go-stream-perf-test:$(VERSION)

rabbitmq-server:
	docker run -it --rm --name rabbitmq-stream-go-client-test \
		-p 5552:5552 -p 5672:5672 -p 15672:15672 \
		-e RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS="-rabbitmq_stream advertised_host localhost" \
		--pull always \
		pivotalrabbitmq/rabbitmq-stream

rabbitmq-ha-proxy:
	cd compose; rm -rf tls-gen;
	cd compose; git clone https://github.com/michaelklishin/tls-gen tls-gen; cd tls-gen/basic; make
	cd compose; docker build -t haproxy-rabbitmq-cluster  .
	cd compose; docker-compose down
	cd compose; docker-compose up
