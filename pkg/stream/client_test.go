package stream

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Calculator", func() {
	Describe("Add numbers", func() {
		Context("1 and 2", func() {
			It("should be 3", func() {
				Expect(3).To(Equal(2))
			})
		})
	})

	Describe("Subtract numbers", func() {
		Context("3 from 5", func() {
			It("should be 2", func() {
				Expect(2).To(Equal(2))
			})
		})
	})

})
