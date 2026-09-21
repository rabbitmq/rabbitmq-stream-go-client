package stream

import (
	"io"
	"net"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Abandoned RPC responses", func() {
	newPipeClient := func(rpcTimeout time.Duration) (*Client, net.Conn) {
		client := newClient(connectionParameters{
			tcpParameters: &TCPParameters{},
			rpcTimeout:    rpcTimeout,
		})
		local, peer := net.Pipe()
		client.setSocketConnection(local)
		DeferCleanup(func() {
			_ = local.Close()
			_ = peer.Close()
			client.coordinator.Close()
		})
		return client, peer
	}

	expectDiscarded := func(client *Client, response *Response) {
		client.coordinator.mutex.Lock()
		registered := len(client.coordinator.responses)
		client.coordinator.mutex.Unlock()
		Expect(registered).To(BeZero())

		// A frame reader that already looked the response up must still be able
		// to send into it.
		Expect(func() {
			response.code <- Code{id: responseCodeOk}
			response.data <- "late response"
		}).NotTo(Panic())
	}

	It("returns a failed write immediately and discards its response", func() {
		client, peer := newPipeClient(time.Hour)
		Expect(peer.Close()).To(Succeed())
		response := client.coordinator.NewResponse(commandMetadata)

		done := make(chan responseError, 1)
		go func() {
			done <- client.handleWrite([]byte("request"), response)
		}()

		var err responseError
		Eventually(done, time.Second).Should(Receive(&err))
		Expect(err.Err).To(HaveOccurred())
		expectDiscarded(client, response)
	})

	DescribeTable("discards a response abandoned by an RPC timeout",
		func(removeResponse bool) {
			client, peer := newPipeClient(10 * time.Millisecond)
			go func() { _, _ = io.Copy(io.Discard, peer) }()
			response := client.coordinator.NewResponse(commandMetadata)

			err := client.handleWriteWithResponse([]byte("request"), response, removeResponse)

			Expect(err.Err).To(MatchError(ContainSubstring("timeout")))
			expectDiscarded(client, response)
		},
		Entry("when the response is removed by the write", true),
		Entry("when the caller removes the response later", false),
	)

	It("discards a response the broker answered with an error code", func() {
		client, peer := newPipeClient(time.Hour)
		go func() { _, _ = io.Copy(io.Discard, peer) }()
		response := client.coordinator.NewResponse(commandMetadata)

		// A handler sends the code before its data payload, so the RPC returns on
		// the error code while the data send is still pending.
		response.code <- Code{id: responseCodeStreamDoesNotExist}

		err := client.handleWriteWithResponse([]byte("request"), response, false)

		Expect(err.Err).To(MatchError(StreamDoesNotExist))
		expectDiscarded(client, response)
	})

	It("leaves a looked-up response usable after the coordinator is closed", func() {
		coordinator := NewCoordinator()
		response := coordinator.NewResponse(commandMetadata)

		// GetResponseById releases the coordinator lock before the handler sends,
		// and Coordinator.Close does not stop the frame reader.
		held, err := coordinator.GetResponseById(uint32(response.correlationid))
		Expect(err).NotTo(HaveOccurred())

		coordinator.Close()

		Expect(func() {
			held.code <- Code{id: responseCodeOk}
			held.data <- "late response"
		}).NotTo(Panic())
	})
})
