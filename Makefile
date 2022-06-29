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
	go install honnef.co/go/tools/cmd/staticcheck@latest
check: $(STATICCHECK)
	$(STATICCHECK) ./pkg/stream

GOMOCK ?= $(GOBIN)/mockgen
GOMOCK_VERSION ?= v1.6.0
$(GOMOCK):
	go install github.com/golang/mock/mockgen@$(GOMOCK_VERSION)

.PHONY: gomock
gomock: $(GOMOCK)

test: vet fmt check
	go test -race -tags debug -v -cpu 2 ./pkg/stream -coverprofile coverage.txt -covermode atomic -ginkgo.v -timeout=2m

build-all: vet fmt check build-darwin build-windows build-linux

integration-test: vet fmt check
	go test -race -tags debug -v -cpu 2 ./pkg/system_integration -coverprofile coverage.txt -covermode atomic -timeout 99999s -ginkgo.v

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
	go build -ldflags=$(LDFLAGS) -o bin/stream-perf-test perfTest/perftest.go

BUILDKIT ?= docker
perf-test-docker-build: perf-test-build
	$(BUILDKIT) build -t pivotalrabbitmq/go-stream-perf-test:$(VERSION) .

perf-test-docker-push: perf-test-docker-build
	$(BUILDKIT) push pivotalrabbitmq/go-stream-perf-test:$(VERSION)

RABBITMQ_OCI ?= pivotalrabbitmq/rabbitmq-stream
BUILDKIT_RUN_ARGS ?= --pull always
.PHONY: rabbitmq-server
rabbitmq-server:
	$(BUILDKIT) run -it --rm --name rabbitmq-stream-go-client-test \
		-p 5552:5552 -p 5672:5672 -p 15672:15672 \
		-e RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS="-rabbitmq_stream advertised_host localhost" \
		$(BUILDKIT_RUN_ARGS) \
		$(RABBITMQ_OCI)

rabbitmq-ha-proxy:
	cd compose/ha_tls; rm -rf tls-gen;
	cd compose/ha_tls; git clone https://github.com/michaelklishin/tls-gen tls-gen; cd tls-gen/basic; make
	mv compose/ha_tls/tls-gen/basic/result/server_*_certificate.pem compose/ha_tls/tls-gen/basic/result/server_certificate.pem
	mv compose/ha_tls/tls-gen/basic/result/server_*key.pem compose/ha_tls/tls-gen/basic/result/server_key.pem
	cd compose/ha_tls; docker build -t haproxy-rabbitmq-cluster  .
	cd compose/ha_tls; docker-compose down
	cd compose/ha_tls; docker-compose up -d

rabbitmq-server-tls:
	cd compose/tls; rm -rf tls-gen;
	cd compose/tls; git clone https://github.com/michaelklishin/tls-gen tls-gen; cd tls-gen/basic; make
	mv compose/tls/tls-gen/basic/result/server_*_certificate.pem compose/tls/tls-gen/basic/result/server_certificate.pem
	mv compose/tls/tls-gen/basic/result/server_*key.pem compose/tls/tls-gen/basic/result/server_key.pem
	docker run -it --rm --name rabbitmq-stream-go-client-test \
		-p 5552:5552 -p 5672:5672 -p 5671:5671 -p 5551:5551 -p 15672:15672 \
		-v  $(shell pwd)/compose/tls/conf/:/etc/rabbitmq/ -v $(shell pwd)/compose/tls/tls-gen/basic/result/:/certs \
		-e RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS="-rabbitmq_stream advertised_host localhost" \
		--pull always \
		pivotalrabbitmq/rabbitmq-stream 	




