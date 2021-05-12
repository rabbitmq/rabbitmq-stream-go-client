package stream

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"sync"
	"time"
)

var _ = Describe("Utils", func() {

	It("Timeout calls No Error", func() {
		response := newResponse(lookUpCommand(commandUnitTest))
		response.correlationid = 9
		var wg sync.WaitGroup
		wg.Add(1)
		go func(res *Response) {
			err := waitCodeWithDefaultTimeOut(res)
			Expect(err).ToNot(HaveOccurred())
			wg.Done()
		}(response)
		time.Sleep(200 * time.Millisecond)
		response.code <- Code{
			id: responseCodeOk,
		}

		wg.Wait()
	})

	It("Timeout calls No Error", func() {
		response := newResponse(lookUpCommand(commandUnitTest))
		response.correlationid = 9
		var wg sync.WaitGroup
		wg.Add(1)
		go func(res *Response) {
			err := waitCodeWithDefaultTimeOut(res)
			Expect(err).To(HaveOccurred())
			wg.Done()
		}(response)

		wg.Wait()
	})

})
