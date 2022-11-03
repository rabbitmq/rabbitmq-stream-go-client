
GO ?= $(shell which go)
define GO_TOOLS
"github.com/golang/mock/mockgen" \
"github.com/onsi/ginkgo/v2/ginkgo"
endef

.PHONY: install-tools
install-tools:
	$(GO) install -v -mod=mod $(GO_TOOLS)

.PHONY: go-generate-mocks
go-generate-mocks:
	mockgen -destination=./pkg/stream/mock_conn_test.go -package=stream_test net Conn
