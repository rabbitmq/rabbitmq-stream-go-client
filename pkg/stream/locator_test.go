package stream

import (
	"context"
	"errors"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
	"github.com/rabbitmq/rabbitmq-stream-go-client/v2/pkg/raw"
	"log/slog"
	"time"
)

var _ = Describe("Locator", func() {
	Describe("Operations", func() {
		var (
			loc           *locator
			logger        *slog.Logger
			backOffPolicy = func(_ int) time.Duration {
				return time.Millisecond * 10
			}
		)

		BeforeEach(func() {
			logger = slog.New(slog.NewTextHandler(GinkgoWriter, &slog.HandlerOptions{Level: slog.LevelDebug}))

			loc = &locator{
				log:                  logger,
				shutdownNotification: make(chan struct{}),
				rawClientConf:        raw.ClientConfiguration{},
				client:               nil,
				isSet:                true,
				clientClose:          nil,
				retryPolicy:          backOffPolicy,
			}
		})

		It("reconnects", func() {
			Skip("this needs a real rabbit to test, or close enough to real rabbit")
		})

		When("there is an error", func() {

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

		})

		Context("logging", func() {
			var logBuffer *gbytes.Buffer

			BeforeEach(func() {
				logBuffer = gbytes.NewBuffer()
				GinkgoWriter.TeeTo(logBuffer)
			})

			It("emits a log when locator operation starts", func() {
				_ = loc.locatorOperation(func(_ *locator, _ ...any) []any {
					return []any{errors.New("oopsie")}
				})

				Eventually(logBuffer).Within(time.Second).Should(gbytes.Say("starting locator operation"))
			})

			It("emits a log when locator operation succeeds", func() {
				_ = loc.locatorOperation(func(_ *locator, _ ...any) []any {
					return []any{nil}
				})

				Eventually(logBuffer).Within(time.Second).Should(gbytes.Say("locator operation succeed"))
			})

			It("emits a log on error", func() {
				_ = loc.locatorOperation(func(_ *locator, _ ...any) []any {
					return []any{errors.New("oopsie")}
				})
				Eventually(logBuffer).Within(time.Second).Should(gbytes.Say("error in locator operation"))
				Eventually(logBuffer).Within(time.Second).Should(gbytes.Say("error"))
				Eventually(logBuffer).Within(time.Second).Should(gbytes.Say("oopsie"))
			})

			It("emits a log on non-retryable error", func() {
				var runs int
				_ = loc.locatorOperation(func(_ *locator, _ ...any) []any {
					runs += 1
					return []any{context.DeadlineExceeded}
				})
				Expect(runs).To(BeNumerically("==", 1))
				Eventually(logBuffer).Within(time.Second).Should(gbytes.Say("locator operation failed with non-retryable error"))
				Eventually(logBuffer).Within(time.Second).Should(gbytes.Say("error"))
				Eventually(logBuffer).Within(time.Second).Should(gbytes.Say("context deadline exceeded"))
			})
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
