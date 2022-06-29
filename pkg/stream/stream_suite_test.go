package stream_test

import (
	"fmt"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"net/http"
	"testing"
)

const testVhost = "rabbitmq-streams-go-test"

func TestStream(t *testing.T) {
	defer GinkgoRecover()
	RegisterFailHandler(Fail)
	RunSpecs(t, "Go-streaming-client")
}

var _ = SynchronizedBeforeSuite(func() []byte {
	err := createVhost(testVhost)
	Expect(err).NotTo(HaveOccurred())
	return nil
}, func(_ []byte) {})

var _ = SynchronizedAfterSuite(func() {}, func() {
	Expect(deleteVhost(testVhost)).NotTo(HaveOccurred())
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
