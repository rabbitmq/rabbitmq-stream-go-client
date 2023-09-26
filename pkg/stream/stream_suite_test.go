package stream_test

import (
	"golang.org/x/exp/slog"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var logger *slog.Logger

func TestStream(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Stream Suite")
}

var _ = BeforeSuite(func() {
	h := slog.HandlerOptions{
		Level: slog.LevelDebug,
	}.NewTextHandler(GinkgoWriter)
	logger = slog.New(h)
})
