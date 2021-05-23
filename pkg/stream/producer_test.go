package stream

import (
	"context"
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"sync"
	"sync/atomic"
	"time"
)

var testProducerStream string

var _ = Describe("Streaming Producers", func() {

	BeforeEach(func() {
		time.Sleep(200 * time.Millisecond)
	})
	AfterEach(func() {
	})

	BeforeEach(func() {
		testProducerStream = uuid.New().String()
		err := testEnvironment.DeclareStream(testProducerStream, nil)
		Expect(err).NotTo(HaveOccurred())

	})
	AfterEach(func() {
		err := testEnvironment.DeleteStream(testProducerStream)
		Expect(err).NotTo(HaveOccurred())

	})

	It("newProducer/Close Publisher", func() {
		producer, err := testEnvironment.NewProducer(testProducerStream, nil, nil)
		Expect(err).NotTo(HaveOccurred())
		err = producer.Close()
		Expect(err).NotTo(HaveOccurred())
	})

	It("newProducer/Publish/Close Publisher", func() {
		producer, err := testEnvironment.NewProducer(testProducerStream, nil, nil)
		Expect(err).NotTo(HaveOccurred())

		_, err = producer.BatchPublish(context.TODO(), CreateArrayMessagesForTesting(5)) // batch send
		Expect(err).NotTo(HaveOccurred())
		// we can't close the subscribe until the publish is finished
		time.Sleep(500 * time.Millisecond)
		err = producer.Close()
		Expect(err).NotTo(HaveOccurred())
	})

	It("Multi-thread newProducer/Publish", func() {
		var wg sync.WaitGroup
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func(wg *sync.WaitGroup) {
				defer wg.Done()
				producer, err := testEnvironment.NewProducer(testProducerStream, nil, nil)
				Expect(err).NotTo(HaveOccurred())

				_, err = producer.BatchPublish(context.TODO(), CreateArrayMessagesForTesting(5)) // batch send
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
		_, err := testEnvironment.NewProducer("notExistingStream", nil, nil)
		Expect(err).
			To(Equal(StreamDoesNotExist))
	})

	It("Publish Confirmation", func() {
		var messagesCount int32 = 0
		chConfirm := make(chan []int64, 1)
		go func(ch chan []int64) {
			ids := <-ch
			atomic.AddInt32(&messagesCount, int32(len(ids)))
		}(chConfirm)

		producer, err := testEnvironment.NewProducer(testProducerStream, chConfirm, nil)
		Expect(err).NotTo(HaveOccurred())
		_, err = producer.BatchPublish(context.TODO(), CreateArrayMessagesForTesting(14))
		Expect(err).NotTo(HaveOccurred())
		time.Sleep(100 * time.Millisecond)
		Expect(atomic.LoadInt32(&messagesCount)).To(Equal(int32(14)))

		err = producer.Close()
		Expect(err).NotTo(HaveOccurred())
	})

	//It("PublishError handler", func() {
	//	producer, Err := testEnvironment.ProducerOptions().Stream(testProducerStream).Build()
	//	Expect(Err).NotTo(HaveOccurred())
	//	//countPublishError := int32(0)
	//	testEnvironment.PublishErrorListener = func(publisherId uint8, publishingId int64, code uint16) {
	//		errString := lookErrorCode(code)
	//		//atomic.AddInt32(&countPublishError, 1)
	//		Expect(errString).
	//			To(ContainSubstring("Code publisher does not exist"))
	//	}
	//	_, Err = producer.BatchPublish(nil, CreateArrayMessagesForTesting(10)) // batch send
	//	producer.Close()
	//	//_, Err = producer.BatchPublish(nil, CreateArrayMessagesForTesting(2)) // batch send
	//	time.Sleep(700 * time.Millisecond)
	//
	//	testEnvironment.PublishErrorListener = nil
	//	//Expect(atomic.LoadInt32(&countPublishError)).To(Equal(int32(2)))
	//
	//})

})
