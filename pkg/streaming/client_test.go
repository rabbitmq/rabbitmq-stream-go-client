package streaming

import (
	"fmt"
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"time"
)

var testClient *Client
var testStreamName string
var _ = BeforeSuite(func() {
	client, err := NewClientCreator().Connect()
	testClient = client
	Expect(err).NotTo(HaveOccurred())
	testStreamName = uuid.New().String()
})

var _ = AfterSuite(func() {
	testClient.Close()
	time.Sleep(500 * time.Millisecond)
	Expect(testClient.coordinator.ProducersCount()).To(Equal(0))
	Expect(testClient.coordinator.ResponsesCount()).To(Equal(0))
	Expect(testClient.coordinator.ConsumersCount()).To(Equal(0))
})

var _ = Describe("Streaming testClient", func() {
	BeforeEach(func() {

	})
	AfterEach(func() {
	})

	Describe("Streaming testClient", func() {
		It("Connection Authentication Failure", func() {
			_, err := NewClientCreator().
				Uri("rabbitmq-StreamCreator://wrong_user:wrong_password@localhost:5551/%2f").
				Connect()
			Expect(fmt.Sprintf("%s", err)).
				To(ContainSubstring("Authentication Failure"))
		})

		It("Connection Fail Vhost", func() {
			_, err := NewClientCreator().
				Uri("rabbitmq-StreamCreator://guest:guest@localhost:5551/VHOSTNOEXIST").
				Connect()
			Expect(fmt.Sprintf("%s", err)).
				To(ContainSubstring("VirtualHost access failure"))
		})

		It("Connection No Endpoint", func() {
			_, err := NewClientCreator().
				Uri("rabbitmq-StreamCreator://g:g@noendpoint:5551/%2f").
				Connect()
			Expect(err).To(HaveOccurred())
		})

		It("Create Stream", func() {
			err := testClient.StreamCreator().Stream(testStreamName).Create()
			Expect(err).NotTo(HaveOccurred())
		})

		It("Create Stream with parameter MaxLengthBytes", func() {
			streamP := uuid.New().String()
			err := testClient.StreamCreator().Stream(streamP).
				MaxLengthBytes(ByteCapacity{}.MB(100)).Create()
			Expect(err).NotTo(HaveOccurred())
			err = testClient.DeleteStream(streamP)
			Expect(err).NotTo(HaveOccurred())

		})

		It("Create Stream with parameter MaxLengthBytes Error", func() {
			streamP := uuid.New().String()
			err := testClient.StreamCreator().Stream(streamP).
				MaxLengthBytes(ByteCapacity{}.From("not_a_valid_value")).Create()
			Expect(fmt.Sprintf("%s", err)).
				To(ContainSubstring("Invalid unit size format"))
		})

		It("Create Stream with parameter MaxAge", func() {
			streamP := uuid.New().String()
			err := testClient.StreamCreator().Stream(streamP).
				MaxAge(120 * time.Hour).Create()
			Expect(err).NotTo(HaveOccurred())
			err = testClient.DeleteStream(streamP)
			Expect(err).NotTo(HaveOccurred())

		})

		It("Delete Stream", func() {
			err := testClient.DeleteStream(testStreamName)
			Expect(err).NotTo(HaveOccurred())
		})
		It("Create two times Stream", func() {
			err := testClient.StreamCreator().Stream(testStreamName).Create()
			Expect(err).NotTo(HaveOccurred())
			err = testClient.StreamCreator().Stream(testStreamName).Create()
			Expect(err).To(HaveOccurred())
			Expect(fmt.Sprintf("%s", err)).
				To(ContainSubstring("Stream already exists"))
			err = testClient.DeleteStream(testStreamName)
			Expect(err).NotTo(HaveOccurred())
		})

		It("Create two times Stream precondition fail", func() {
			err := testClient.StreamCreator().Stream(testStreamName).Create()
			Expect(err).NotTo(HaveOccurred())
			err = testClient.StreamCreator().Stream(testStreamName).
				MaxLengthBytes(ByteCapacity{}.MB(100)).
				Create()
			Expect(err).To(HaveOccurred())
			Expect(fmt.Sprintf("%s", err)).
				To(ContainSubstring("Precondition Failed"))
			err = testClient.DeleteStream(testStreamName)
			Expect(err).NotTo(HaveOccurred())
		})

		It("Create empty Stream  fail", func() {
			err := testClient.StreamCreator().Stream("").Create()
			Expect(err).To(HaveOccurred())
			Expect(fmt.Sprintf("%s", err)).
				To(ContainSubstring("stream name can't be empty"))
		})

	})
})
