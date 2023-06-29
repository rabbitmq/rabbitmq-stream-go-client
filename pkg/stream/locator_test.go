package stream

import (
	"context"
	"errors"
	"github.com/gsantomaggio/rabbitmq-stream-go-client/pkg/raw"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"golang.org/x/exp/slog"
	"time"
)

var _ = Describe("Locator", func() {
	Describe("Operations", func() {
		var (
			logger        *slog.Logger
			backOffPolicy = func(_ int) time.Duration {
				return time.Millisecond * 10
			}
		)

		BeforeEach(func() {
			logger = slog.New(slog.NewTextHandler(GinkgoWriter))
		})

		It("reconnects", func() {
			Skip("this needs a real rabbit to test, or close enough to real rabbit")
		})

		When("there is an error", func() {
			var (
				loc *locator
			)

			BeforeEach(func() {
				loc = &locator{
					log:                  logger,
					shutdownNotification: make(chan struct{}),
					rawClientConf:        raw.ClientConfiguration{},
					client:               nil,
					isSet:                true,
					clientClose:          nil,
					backOffPolicy:        backOffPolicy,
				}
			})

			It("retries the operation on retryable errors", func() {
				var runs int
				_ = loc.locatorOperation(func(_ *locator, _ ...any) []any {
					runs += 1
					return []any{errors.New("oopsie")}
				})
				Expect(runs).To(BeNumerically("==", 3))
			})

			It("gives up on non-retryable errors", func() {
				var runs int
				_ = loc.locatorOperation(func(_ *locator, _ ...any) []any {
					runs += 1
					return []any{context.DeadlineExceeded}
				})
				Expect(runs).To(BeNumerically("==", 1))
			})

			It("bubbles up the error", func() {
				r := loc.locatorOperation(func(_ *locator, _ ...any) []any {
					return []any{errors.New("oopsie")}
				})
				Expect(r).To(HaveLen(1)) // not all result slices with error have only 1 element tho
				Expect(r[0]).To(BeAssignableToTypeOf(errors.New("an error")))
				Expect(r[0]).To(MatchError("oopsie"))
			})

			// TODO: add a test for logs
		})
	})

	Describe("Utils", func() {
		var (
			discardLogger = slog.New(discardHandler{})
		)
		It("determines if server version is 3.11 or more", func() {
			conf, err := raw.NewClientConfiguration("")
			Expect(err).ToNot(HaveOccurred())
			versionVal := "3.10.0"
			conf.SetServerProperties("version", versionVal)

			loc := newLocator(*conf, discardLogger)
			Expect(loc.isServer311orMore()).To(BeFalse(), "expected %s to be lower than 3.11.0", versionVal)

			versionVal = "3.11.0"
			conf.SetServerProperties("version", versionVal)
			Expect(loc.isServer311orMore()).To(BeTrue(), "expected %s to be greater or equal than 3.11.0", versionVal)

			versionVal = "3.11.10"
			conf.SetServerProperties("version", versionVal)
			Expect(loc.isServer311orMore()).To(BeTrue(), "expected %s to be greater or equal than 3.11.0", versionVal)

			versionVal = "3.12.0"
			conf.SetServerProperties("version", versionVal)
			Expect(loc.isServer311orMore()).To(BeTrue(), "expected %s to be greater or equal than 3.11.0", versionVal)

			versionVal = "3.11.0-alpha.2"
			conf.SetServerProperties("version", versionVal)
			Expect(loc.isServer311orMore()).To(BeFalse(), "expected %s to be lower than 3.11.0", versionVal)

			versionVal = "3.13.0-beta.1"
			conf.SetServerProperties("version", versionVal)
			Expect(loc.isServer311orMore()).To(BeTrue(), "expected %s to be greater or equal than 3.11.0", versionVal)
		})
	})
})
