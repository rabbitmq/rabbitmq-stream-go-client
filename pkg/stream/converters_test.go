package stream

import (
	"fmt"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Converters", func() {

	It("Converter from number", func() {
		Expect(ByteCapacity{}.B(100).bytes).To(Equal(int64(100)))
		Expect(ByteCapacity{}.KB(1).bytes).To(Equal(int64(1000)))
		Expect(ByteCapacity{}.MB(1).bytes).To(Equal(int64(1000 * 1000)))
		Expect(ByteCapacity{}.GB(1).bytes).To(Equal(int64(1000 * 1000 * 1000)))
		Expect(ByteCapacity{}.TB(1).bytes).To(Equal(int64(1000 * 1000 * 1000 * 1000)))
	})

	It("Converter from string", func() {
		v := ByteCapacity{}.From("1KB")
		Expect(v.error).NotTo(HaveOccurred())
		Expect(v.bytes).To(Equal(int64(1000)))

		v = ByteCapacity{}.From("1MB")
		Expect(v.error).NotTo(HaveOccurred())
		Expect(v.bytes).To(Equal(int64(1000 * 1000)))

		v = ByteCapacity{}.From("1GB")
		Expect(v.error).NotTo(HaveOccurred())
		Expect(v.bytes).To(Equal(int64(1000 * 1000 * 1000)))

		v = ByteCapacity{}.From("1tb")
		Expect(v.error).NotTo(HaveOccurred())
		Expect(v.bytes).To(Equal(int64(1000 * 1000 * 1000 * 1000)))
	})

	It("Converter from string logError", func() {
		v := ByteCapacity{}.From("10LL")
		Expect(fmt.Sprintf("%s", v.error)).
			To(ContainSubstring("Invalid unit size format"))

		v = ByteCapacity{}.From("aGB")
		Expect(fmt.Sprintf("%s", v.error)).
			To(ContainSubstring("Invalid number format"))
	})

})
