package streaming

import (
	"fmt"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Converters", func() {

	It("Converter from number", func() {
		Expect(ByteCapacity{}.B(100)).To(Equal(int64(100)))
		Expect(ByteCapacity{}.KB(1)).To(Equal(int64(1000)))
		Expect(ByteCapacity{}.MB(1)).To(Equal(int64(1000 * 1000)))
		Expect(ByteCapacity{}.GB(1)).To(Equal(int64(1000 * 1000 * 1000)))
		Expect(ByteCapacity{}.TB(1)).To(Equal(int64(1000 * 1000 * 1000 * 1000)))
	})

	It("Converter from string", func() {
		v, err := ByteCapacity{}.From("1KB")
		Expect(err).NotTo(HaveOccurred())
		Expect(v).To(Equal(int64(1000)))

		v, err = ByteCapacity{}.From("1MB")
		Expect(err).NotTo(HaveOccurred())
		Expect(v).To(Equal(int64(1000 * 1000)))

		v, err = ByteCapacity{}.From("1GB")
		Expect(err).NotTo(HaveOccurred())
		Expect(v).To(Equal(int64(1000 * 1000 * 1000)))
	})

	It("Converter from string error", func() {
		_, err := ByteCapacity{}.From("10LL")
		Expect(fmt.Sprintf("%s", err)).
			To(ContainSubstring("Invalid unit size format"))

		_, err = ByteCapacity{}.From("aGB")
		Expect(fmt.Sprintf("%s", err)).
			To(ContainSubstring("Invalid number format"))

	})

})
