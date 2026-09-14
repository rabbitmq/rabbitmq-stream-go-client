package stream

import (
	"encoding/binary"
	"io"
	"net"
	"sync/atomic"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// metadataRetryConn answers the first metadata request with a stream that has
// no leader yet and fails every later write, so the leader retry sees a failed
// metadata RPC without needing a broker.
type metadataRetryConn struct {
	net.Conn
	client   *Client
	stream   string
	requests atomic.Int32
}

func (c *metadataRetryConn) Write(data []byte) (int, error) {
	if c.requests.Add(1) > 1 {
		return 0, io.ErrClosedPipe
	}

	// header: length (4), command (2), version (2), correlation id (4)
	response, err := c.client.coordinator.GetResponseById(binary.BigEndian.Uint32(data[8:12]))
	if err != nil {
		return 0, err
	}
	metadata := StreamsMetadata{}.New()
	metadata.Add(c.stream, responseCodeOk, nil, nil)
	response.code <- Code{id: responseCodeOk}
	response.data <- metadata
	return len(data), nil
}

var _ = Describe("StreamMetaData leader retry", func() {
	It("returns StreamMetadataFailure when a leader retry fails", func() {
		client := newClient(connectionParameters{
			connectionName: "metadata-retry-client",
			tcpParameters:  &TCPParameters{},
			rpcTimeout:     200 * time.Millisecond,
		})
		local, peer := net.Pipe()
		conn := &metadataRetryConn{Conn: local, client: client, stream: "retry-metadata"}
		client.setSocketConnection(conn)
		client.socket.setOpen()
		DeferCleanup(func() {
			_ = local.Close()
			_ = peer.Close()
			client.coordinator.Close()
		})

		env := &Environment{
			options: &EnvironmentOptions{TCPParameters: client.tcpParameters},
			locator: newLocator(client),
		}

		done := make(chan error, 1)
		go func() {
			defer GinkgoRecover()
			_, err := env.StreamMetaData("retry-metadata")
			done <- err
		}()

		var err error
		Eventually(done, 5*time.Second).Should(Receive(&err))
		Expect(err).To(MatchError(StreamMetadataFailure))
		Expect(conn.requests.Load()).To(BeEquivalentTo(2))
	})
})
