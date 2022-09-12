package internal

import (
	"bufio"
	"bytes"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

const FakeCommandKey = uint16(1)

type FakeCommandRequest struct {
}

func (f *FakeCommandRequest) Write(writer *bufio.Writer) (int, error) {
	return 99, nil
}

func (f *FakeCommandRequest) GetKey() uint16 {
	return FakeCommandKey
}

func (f *FakeCommandRequest) SizeNeeded() int {
	return 99
}

func (f *FakeCommandRequest) SetCorrelationId(id uint32) {

}

func (f *FakeCommandRequest) GetCorrelationId() uint32 {
	return 15
}

var _ = Describe("Commands", func() {
	Describe("Validate Commands Structs", func() {

		It("header from command write", func() {
			fakeCommandW := &FakeCommandRequest{}
			header := NewHeaderRequest(fakeCommandW)
			Expect(header.Command).To(Equal(FakeCommandKey))
			Expect(header.length).To(Equal(99))
			Expect(header.version).To(Equal(Version1))
			buff := bytes.NewBuffer(make([]byte, 0, 8))
			x := bufio.NewWriter(buff)

			written, err := header.Write(x)
			Expect(err).To(BeNil())
			Expect(x.Flush()).To(BeNil())
			Expect(written).To(Equal(8))
			fakeCommandR := NewHeaderResponse()
			Expect(fakeCommandR.Read(bufio.NewReader(buff))).To(BeNil())
			Expect(fakeCommandR.Command).To(Equal(FakeCommandKey))
			Expect(fakeCommandR.length).To(Equal(99))
			Expect(fakeCommandR.version).To(Equal(Version1))

		})

	})
})
