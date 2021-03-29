package streaming

import (
	"fmt"
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"sync"
	"time"
)

var testProducerStream string

var _ = Describe("Streaming Producers", func() {

	BeforeEach(func() {
		testProducerStream = uuid.New().String()
		err := testClient.StreamCreator().Stream(testProducerStream).Create()
		Expect(err).NotTo(HaveOccurred())

	})
	AfterEach(func() {
		err := testClient.DeleteStream(testProducerStream)
		Expect(err).NotTo(HaveOccurred())

	})

	It("NewProducer/Close Publisher", func() {
		producer, err := testClient.ProducerCreator().Stream(testProducerStream).Build()
		Expect(err).NotTo(HaveOccurred())
		err = producer.Close()
		Expect(err).NotTo(HaveOccurred())
	})

	It("NewProducer/Publish/UnSubscribe Publisher", func() {
		producer, err := testClient.ProducerCreator().Stream(testProducerStream).Build()
		Expect(err).NotTo(HaveOccurred())

		_, err = producer.BatchPublish(nil, CreateArrayMessagesForTesting(5)) // batch send
		Expect(err).NotTo(HaveOccurred())
		// we can't close the subscribe until the publish is finished
		time.Sleep(500 * time.Millisecond)
		err = producer.Close()
		Expect(err).NotTo(HaveOccurred())
	})

	It("Multi-thread NewProducer/Publish/UnSubscribe", func() {
		var wg sync.WaitGroup
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func(wg *sync.WaitGroup) {
				defer wg.Done()
				producer, err := testClient.ProducerCreator().Stream(testProducerStream).Build()
				Expect(err).NotTo(HaveOccurred())

				_, err = producer.BatchPublish(nil, CreateArrayMessagesForTesting(5)) // batch send
				Expect(err).NotTo(HaveOccurred())
				// we can't close the subscribe until the publish is finished
				time.Sleep(500 * time.Millisecond)
				err = producer.Close()
				Expect(err).NotTo(HaveOccurred())
			}(&wg)
		}
		wg.Wait()
	})

	It("Not found NotExistingStream", func() {
		localClient, err := NewClientCreator().Connect()
		Expect(err).NotTo(HaveOccurred())
		producer, err := localClient.ProducerCreator().Stream("notExistingStream").Build()
		Expect(fmt.Sprintf("%s", err)).
			To(ContainSubstring("Stream does not exist"))
		err = producer.Close()
		Expect(fmt.Sprintf("%s", err)).
			To(ContainSubstring("Code publisher does not exist"))


	})

})
