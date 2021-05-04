package streaming

import (
	"bufio"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
)

type Socket struct {
	connection net.Conn
	connected  bool
	writer     *bufio.Writer
	mutex      *sync.Mutex
	closed     int32
	destructor *sync.Once
}

func (sck *Socket) SetOpen() {
	atomic.StoreInt32(&sck.closed, 0)
}

func (sck *Socket) isOpen() bool {
	return atomic.LoadInt32(&sck.closed) == 0
}
func (sck *Socket) shutdown(err error) {
	atomic.StoreInt32(&sck.closed, 1)

	sck.destructor.Do(func() {
		sck.mutex.Lock()
		defer sck.mutex.Unlock()
		err := sck.connection.Close()
		if err != nil {
			WARN("error during close socket: %s", err)
		}
	})

}

func (sck *Socket) SetConnect(value bool) {
	sck.mutex.Lock()
	defer sck.mutex.Unlock()
	sck.connected = value
}

func (sck *Socket) writeAndFlush(buffer []byte) error {
	sck.mutex.Lock()
	defer sck.mutex.Unlock()
	_, err := sck.writer.Write(buffer)
	if err != nil {
		return err
	}
	err = sck.writer.Flush()
	if err != nil {
		return err
	}

	return nil
}

func (c *Client) HandleWrite(buffer []byte, response *Response) error {
	return c.HandleWriteWithResponse(buffer, response, true)
}

func (c *Client) HandleWriteWithResponse(buffer []byte, response *Response, removeResponse bool) error {
	result := c.socket.writeAndFlush(buffer)
	resultCode := WaitCodeWithDefaultTimeOut(response)
	/// we need to remove the response before evaluate the
	// buffer errSocket
	if removeResponse {
		result = c.coordinator.RemoveResponseById(response.correlationid)
	}

	if result != nil {
		// we just log
		fmt.Printf("Error HandleWrite %s", result)
	}

	return resultCode
}
