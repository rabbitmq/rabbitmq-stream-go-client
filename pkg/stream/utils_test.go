package stream

import (
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Utils", func() {

	It("Timeout calls No Error", func() {
		response := newResponse(lookUpCommand(commandUnitTest))
		response.correlationid = 9
		var wg sync.WaitGroup
		wg.Add(1)
		go func(res *Response) {
			defer GinkgoRecover()
			err := waitCodeWithDefaultTimeOut(res)
			Expect(err.Err).ToNot(HaveOccurred())
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
			defer GinkgoRecover()
			err := waitCodeWithDefaultTimeOut(res)
			Expect(err.Err).To(HaveOccurred())
			wg.Done()
		}(response)

		wg.Wait()
	})

})
