package stream

import (
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

const testVhost = "rabbitmq-streams-go-test"

var testEnvironment *Environment
var testStreamName string

var _ = SynchronizedBeforeSuite(func() []byte {
	err := createVhost(testVhost)
	Expect(err).NotTo(HaveOccurred())
	return nil
}, func(_ []byte) {
	//SetLevelInfo(logs.DEBUG)
	client, err := NewEnvironment(nil)
	testEnvironment = client
	Expect(err).NotTo(HaveOccurred())
	testStreamName = uuid.New().String()
})

var _ = SynchronizedAfterSuite(func() {
	time.Sleep(400 * time.Millisecond)
	err := testEnvironment.Close()
	Expect(err).NotTo(HaveOccurred())
	time.Sleep(100 * time.Millisecond)

	//Expect(testEnvironment.Coordinators()[0].ProducersCount()).To(Equal(0))
	//Expect(testEnvironment.clientLocator.coordinator.ResponsesCount()).To(Equal(0))
	//Expect(testEnvironment.clientLocator.coordinator.ConsumersCount()).To(Equal(0))
}, func() {
	err := deleteVhost(testVhost)
	Expect(err).NotTo(HaveOccurred())
})

var _ = Describe("Streaming testEnvironment", func() {
	BeforeEach(func() {
		time.Sleep(200 * time.Millisecond)
	})
	AfterEach(func() {
		time.Sleep(200 * time.Millisecond)
	})

	It("Create Stream", func() {
		err := testEnvironment.DeclareStream(testStreamName, nil)
		Expect(err).NotTo(HaveOccurred())
		err = testEnvironment.DeleteStream(testStreamName)
		Expect(err).NotTo(HaveOccurred())
	})

	It("Create Stream with parameter SetMaxLengthBytes", func() {
		streamP := uuid.New().String()

		err := testEnvironment.DeclareStream(streamP,
			&StreamOptions{
				MaxLengthBytes: ByteCapacity{}.GB(2),
			},
		)
		Expect(err).NotTo(HaveOccurred())
		err = testEnvironment.DeleteStream(streamP)
		Expect(err).NotTo(HaveOccurred())

	})

	It("Create Stream with parameter SetMaxLengthBytes Error", func() {
		By("Client Test")
		streamP := uuid.New().String()

		err := testEnvironment.DeclareStream(streamP, NewStreamOptions().
			SetMaxLengthBytes(ByteCapacity{}.From("not_a_valid_value")))

		Expect(fmt.Sprintf("%s", err)).
			To(ContainSubstring("Invalid unit size format"))
	})

	It("Create Stream with parameter SetMaxSegmentSizeBytes", func() {
		streamP := uuid.New().String()

		err := testEnvironment.DeclareStream(streamP,
			&StreamOptions{
				MaxSegmentSizeBytes: ByteCapacity{}.MB(100),
			},
		)
		Expect(err).NotTo(HaveOccurred())
		err = testEnvironment.DeleteStream(streamP)
		Expect(err).NotTo(HaveOccurred())

	})

	It("Create Stream with parameter SetMaxSegmentSizeBytes Error", func() {
		streamP := uuid.New().String()

		err := testEnvironment.DeclareStream(streamP,
			&StreamOptions{
				MaxSegmentSizeBytes: ByteCapacity{}.From("not_a_valid_value"),
			})

		Expect(fmt.Sprintf("%s", err)).
			To(ContainSubstring("Invalid unit size format"))
	})

	It("Create Stream with parameter SetMaxAge", func() {
		streamP := uuid.New().String()
		err := testEnvironment.DeclareStream(streamP, NewStreamOptions().SetMaxAge(120*time.Hour))
		Expect(err).NotTo(HaveOccurred())
		err = testEnvironment.DeleteStream(streamP)
		Expect(err).NotTo(HaveOccurred())

	})

	It("Create two times Stream", func() {
		err := testEnvironment.DeclareStream(testStreamName, nil)
		Expect(err).NotTo(HaveOccurred())
		err = testEnvironment.DeclareStream(testStreamName, nil)
		Expect(err).To(HaveOccurred())
		Expect(err).To(Equal(StreamAlreadyExists))
		err = testEnvironment.DeleteStream(testStreamName)
		Expect(err).NotTo(HaveOccurred())
	})

	It("Create two times Stream precondition fail", func() {
		err := testEnvironment.DeclareStream(testStreamName, nil)
		Expect(err).NotTo(HaveOccurred())
		err = testEnvironment.DeclareStream(testStreamName,
			&StreamOptions{
				MaxAge:         0,
				MaxLengthBytes: ByteCapacity{}.MB(100),
			})
		Expect(err).To(HaveOccurred())
		Expect(err).To(Equal(PreconditionFailed))
		err = testEnvironment.DeleteStream(testStreamName)
		Expect(err).NotTo(HaveOccurred())
	})

	It("Create empty Stream  fail", func() {
		err := testEnvironment.DeclareStream("", nil)
		Expect(err).To(HaveOccurred())
		Expect(fmt.Sprintf("%s", err)).
			To(ContainSubstring("stream Name can't be empty"))
	})
})

func createVhost(vhost string) error {
	return httpCall("PUT", vhostUrl(vhost))
}

func deleteVhost(vhost string) error {
	return httpCall("DELETE", vhostUrl(vhost))
}

func vhostUrl(vhost string) string {
	return fmt.Sprintf("http://guest:guest@localhost:15672/api/vhosts/%s", vhost)
}

func httpCall(method, url string) error {
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		return err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("http error (%d): %s", resp.StatusCode, resp.Status)
	}
	return nil
}
