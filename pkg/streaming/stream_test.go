package streaming_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Stream", func() {
	Describe("Add", func() {

		It("adds two numbers", func() {
			sum := 2 + 3
			Expect(sum).To(Equal(5))
		})
	})
})
