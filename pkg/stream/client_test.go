package stream

import (
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
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
	Expect(testEnvironment.Close()).NotTo(HaveOccurred())
	time.Sleep(100 * time.Millisecond)
}, func() {
	Expect(deleteVhost(testVhost)).NotTo(HaveOccurred())
})

var _ = Describe("Streaming testEnvironment", func() {
	BeforeEach(func() {
		time.Sleep(200 * time.Millisecond)
	})
	AfterEach(func() {
		time.Sleep(200 * time.Millisecond)
	})

	It("Create Stream", func() {
		Expect(testEnvironment.DeclareStream(testStreamName, nil)).
			NotTo(HaveOccurred())
		Expect(testEnvironment.DeleteStream(testStreamName)).NotTo(HaveOccurred())
	})

	It("Create Stream with parameter SetMaxLengthBytes and SetMaxSegmentSizeBytes", func() {
		streamP := uuid.New().String()
		Expect(testEnvironment.DeclareStream(streamP,
			&StreamOptions{
				MaxLengthBytes: ByteCapacity{}.GB(2),
			},
		)).NotTo(HaveOccurred())
		Expect(testEnvironment.DeleteStream(streamP)).
			NotTo(HaveOccurred())

		Expect(testEnvironment.DeclareStream(streamP, NewStreamOptions().
			SetMaxSegmentSizeBytes(ByteCapacity{}.KB(500)))).NotTo(HaveOccurred())
		Expect(testEnvironment.DeleteStream(streamP)).
			NotTo(HaveOccurred())

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
		Expect(testEnvironment.DeclareStream(testStreamName, nil)).NotTo(HaveOccurred())
		err := testEnvironment.DeclareStream(testStreamName, nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(testEnvironment.DeleteStream(testStreamName)).NotTo(HaveOccurred())
	})

	It("Stream Exists", func() {
		exists, err := testEnvironment.StreamExists(testStreamName)
		Expect(err).NotTo(HaveOccurred())
		Expect(exists).To(Equal(false))

		Expect(testEnvironment.DeclareStream(testStreamName, nil)).NotTo(HaveOccurred())
		exists, err = testEnvironment.StreamExists(testStreamName)
		Expect(err).NotTo(HaveOccurred())
		Expect(exists).To(Equal(true))
		Expect(testEnvironment.DeleteStream(testStreamName)).NotTo(HaveOccurred())
	})

	It("Create two times Stream precondition fail", func() {
		Expect(testEnvironment.DeclareStream(testStreamName, nil)).NotTo(HaveOccurred())
		err := testEnvironment.DeclareStream(testStreamName,
			&StreamOptions{
				MaxAge:         0,
				MaxLengthBytes: ByteCapacity{}.MB(100),
			})
		Expect(err).To(HaveOccurred())
		Expect(err).To(Equal(PreconditionFailed))
		Expect(testEnvironment.DeleteStream(testStreamName)).NotTo(HaveOccurred())
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
