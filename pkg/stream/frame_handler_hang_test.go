package stream

import (
	"bufio"
	"bytes"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Frame handlers that must not stall the reader", func() {
	It("completes a stream status frame whose RPC already timed out", func() {
		client := &Client{coordinator: NewCoordinator()}
		response := client.coordinator.NewResponse(commandStreamStatus)

		var body bytes.Buffer
		writeUInt(&body, uint32(response.correlationid))
		writeUShort(&body, responseCodeOk)
		writeUInt(&body, 1)
		writeString(&body, "committed_chunk_id")
		writeLong(&body, 42)

		// Nobody drains the response: this is the state left behind by an RPC
		// that gave up waiting. The handler still has to return, otherwise the
		// whole connection's frame reader is stuck.
		done := make(chan struct{})
		go func() {
			defer GinkgoRecover()
			defer close(done)
			client.streamStatusFrameHandler(&ReaderProtocol{}, bufio.NewReader(&body))
		}()

		Eventually(done, time.Second).Should(BeClosed())
		Expect(<-response.code).To(Equal(Code{id: responseCodeOk}))
		Expect(<-response.data).To(Equal(map[string]int64{"committed_chunk_id": 42}))
	})

	It("ignores a tune frame that has no registered response", func() {
		client := &Client{coordinator: NewCoordinator()}
		client.tuneState.requestedMaxFrameSize = 512 * 1024
		client.tuneState.requestedHeartbeat = 30

		var body bytes.Buffer
		writeUInt(&body, 1048576)
		writeUInt(&body, 60)

		Expect(func() {
			client.handleTune(bufio.NewReader(&body))
		}).NotTo(Panic())
	})
})
