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
	$(GO) install -mod=mod github.com/golang/mock/mockgen

.PHONY: mockgen
mockgen: | $(MOCKGEN)

.PHONY: install-tools
install-tools: ## Install tool dependencies for development
	@printf "$(GREEN)Installing dev-tools$(NORMAL)\n"
	$(GO) install -v -mod=mod $(GO_TOOLS)

### Golang targets

.PHONY: go-mod-tidy
go-mod-tidy: ## Run 'go mod tidy' with compatibility to Go 1.17
	$(GO) mod tidy -go=1.17

.PHONY: go-generate-mocks
go-generate-mocks: | $(MOCKGEN) ## Generate Mocks for testing
	$(MOCKGEN) -destination=./pkg/stream/mock_conn_test.go -package=stream_test net Conn

### build

build: ## Install tool dependencies for development
	$(GO) build -v ./...



### Tests

GINKGO_RUN_FLAGS := -r --randomize-all -p --race

.PHONY: tests
tests: | $(GINKGO) ## Run unit tests. Make sure you install-tools before running this target
	@printf "$(GREEN)Running all tests in parallel$(NORMAL)\n"
	$(GINKGO) $(GINKGO_RUN_FLAGS) $(GINKGO_EXTRA) .
