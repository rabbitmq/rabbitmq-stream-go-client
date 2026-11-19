package ha

import (
	"math"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("BackOffConfiguration", func() {
	Describe("Validate", func() {
		It("accepts the default configuration", func() {
			Expect(NewBackOffConfiguration().Validate()).To(Succeed())
		})

		It("accepts the boundary values", func() {
			cfg := &BackOffConfiguration{MinInterval: 1, MaxInterval: 100}
			Expect(cfg.Validate()).To(Succeed())
		})

		It("rejects a nil configuration", func() {
			var cfg *BackOffConfiguration
			Expect(cfg.Validate()).To(HaveOccurred())
		})

		It("rejects a MinInterval less than 1", func() {
			cfg := &BackOffConfiguration{MinInterval: 0, MaxInterval: 8}
			Expect(cfg.Validate()).To(MatchError(ContainSubstring("MinInterval")))
		})

		It("rejects a MaxInterval greater than 100", func() {
			cfg := &BackOffConfiguration{MinInterval: 1, MaxInterval: 101}
			Expect(cfg.Validate()).To(MatchError(ContainSubstring("MaxInterval")))
		})

		It("rejects a MaxInterval less than MinInterval", func() {
			cfg := &BackOffConfiguration{MinInterval: 5, MaxInterval: 4}
			Expect(cfg.Validate()).To(MatchError(ContainSubstring("MaxInterval")))
		})
	})

	Describe("randomWaitWithBackoff", func() {
		It("returns MinInterval when random is disabled", func() {
			cfg := &BackOffConfiguration{MinInterval: 2, MaxInterval: 4}
			Expect(randomWaitWithBackoff(1, cfg)).To(Equal(2_000))
		})

		It("adds a random jitter below MaxInterval when random is enabled", func() {
			cfg := &BackOffConfiguration{MinInterval: 2, MaxInterval: 4, EnableRandom: true}
			Expect(randomWaitWithBackoff(1, cfg)).To(And(BeNumerically(">=", 2_000), BeNumerically("<", 6_000)))
		})

		It("falls back to the defaults when the configuration is nil", func() {
			Expect(randomWaitWithBackoff(1, nil)).To(And(BeNumerically(">=", 3_000), BeNumerically("<", 11_000)))
		})

		It("caps the wait at MaxInterval however many attempts have failed", func() {
			cfg := &BackOffConfiguration{MinInterval: 3, MaxInterval: 8}
			for _, attempt := range []int{3, 10, 53, 54, 60, 63, 64, 65, 100, 1_000, math.MaxInt} {
				Expect(randomWaitWithBackoff(attempt, cfg)).To(Equal(8_000), "attempt %d", attempt)
			}
		})
	})
})
