SHELL := bash # we want bash behaviour in all shell invocations
PLATFORM := $(shell uname)
platform := $(shell echo $(PLATFORM) | tr A-Z a-z)

# https://stackoverflow.com/questions/4842424/list-of-ansi-color-escape-sequences
RED := \033[1;31m
GREEN := \033[1;32m
YELLOW := \033[1;33m
BOLD := \033[1m
NORMAL := \033[0m

HELP_TARGET_DEPTH ?= \#\#
.PHONY: help
help:
	@awk -F':+ |$(HELP_TARGET_DEPTH)' '/^[^.][0-9a-zA-Z._%-]+:+.+$(HELP_TARGET_DEPTH).+$$/ { printf "\033[36m%-26s\033[0m %s\n", $$1, $$3 }' $(MAKEFILE_LIST) \
	| sort

.DEFAULT_GOAL := help

### Tools and setup

GO ?= $(shell which go)
GOPATH ?= $(shell $(GO) env GOPATH)
define GO_TOOLS
"github.com/golang/mock/mockgen" \
"github.com/onsi/ginkgo/v2/ginkgo"
endef

GINKGO ?= $(GOPATH)/bin/ginkgo
$(GINKGO):
	@printf "$(GREEN)Installing ginkgo CLI$(NORMAL)\n"
	$(GO) install -mod=mod github.com/onsi/ginkgo/v2/ginkgo

.PHONY: ginkgo
ginkgo: | $(GINKGO)

MOCKGEN ?= $(GOPATH)/bin/mockgen
$(MOCKGEN):
	@printf "$(GREEN)Installing mockgen CLI$(NORMAL)\n"
	$(GO) install -mod=mod go.uber.org/mock/mockgen

.PHONY: mockgen
mockgen: | $(MOCKGEN)

.PHONY: install-tools
install-tools: ## Install tool dependencies for development
	@printf "$(GREEN)Installing dev-tools$(NORMAL)\n"
	$(GO) install -v -mod=mod $(GO_TOOLS)

### Golang targets

.PHONY: go-mod-tidy
go-mod-tidy: ## Run 'go mod tidy' with compatibility to Go 1.21
	$(GO) mod tidy -go=1.21

.PHONY: go-generate-mocks
go-generate-mocks: | $(MOCKGEN) ## Generate Mocks for testing
	$(MOCKGEN) -destination=./pkg/mock/mock_conn.go -package=mock net Conn
	$(MOCKGEN) -source=pkg/raw/client_types.go -package stream \
		-destination=pkg/stream/mock_raw_client_test.go \
		-mock_names Clienter=MockRawClient \
		pkg/raw Clienter
	$(GO) generate internal/command_types.go

### build

build: ## Just compile all
	$(GO) build -v ./...

### Tests

GINKGO_RUN_SHARED_FLAGS := --randomize-all --race
GINKGO_RUN_FLAGS := -r -p --tags="rabbitmq.stream.test"

.PHONY: tests
tests: | $(GINKGO) ## Run unit tests. Make sure you install-tools before running this target. Use GINKGO_EXTRA to add extra flags to ginkgo
	@printf "$(GREEN)Running all tests in parallel$(NORMAL)\n"
	$(GINKGO) $(GINKGO_RUN_SHARED_FLAGS) $(GINKGO_RUN_FLAGS) --skip-package "e2e" $(GINKGO_EXTRA) .

# Run tests target for CI. Using go run instead of Ginkgo CLI to save some bandwidth by not downloading Ginkgo CLI
tests-ci:
	$(GO) run github.com/onsi/ginkgo/v2/ginkgo \
		$(GINKGO_RUN_SHARED_FLAGS) \
		$(GINKGO_RUN_FLAGS) \
		--randomize-suites \
		--label-filter="!flaky" \
		--skip-package "e2e" \
		--fail-on-pending \
		--keep-going


#### e2e test suite accepts the flags -keep-rabbit-container=true and -rabbit-debug-log=true
#### -keep-rabbit-container=true does not delete the rabbit container after the suite run. It is useful to examine rabbit logs after a test failure
#### -rabbit-debug-log=true enables debug log level in rabbitmq before the tests start
.PHONY: e2e_tests
e2e_tests: | $(GINKGO) ## Run end-to-end tests. Make sure you have a running Docker daemon and Docker socket in /var/run/docker.sock or set RMQ_E2E_SKIP_CONTAINER_START
	@printf "$(GREEN)Running end-to-end tests$(NORMAL)\n"
	$(GINKGO) $(GINKGO_RUN_SHARED_FLAGS) --tags="rabbitmq.stream.e2e" --label-filter "!measurement" $(GINKGO_EXTRA) ./pkg/e2e

.PHONY: benchmarks
benchmarks: | $(GINKGO) ## Run benchmarks. Make sure you have a running Docker. Or set RMQ_E2E_SKIP_CONTAINER_START
	@printf "$(GREEN)Running benchmark tests$(NORMAL)\n"
	$(GINKGO) $(GINKGO_RUN_SHARED_FLAGS) --nodes 1 --tags="rabbitmq.stream.e2e" --label-filter "measurement" ./pkg/e2e

### Containers

CONTAINER_ENGINE ?= docker

.PHONY: start_rabbitmq
start_rabbitmq: ## Start a RabbitMQ container. This is not required for tests. E2E suite will start a container before running tests
	./scripts/start-docker.bash -c $(CONTAINER_ENGINE)

stop_rabbitmq: ## Stop a RabbitMQ
	./scripts/stop-docker.bash -c $(CONTAINER_ENGINE)
