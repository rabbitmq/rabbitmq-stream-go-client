package streaming

import (
	"bufio"
	"fmt"
	"net"
	"sync"
)

type Socket struct {
	connection net.Conn
	connected  bool
	writer     *bufio.Writer
	reader     *bufio.Reader
	mutex      *sync.Mutex
}

func (sck *Socket) SetConnect(value bool) {
	sck.mutex.Lock()
	defer sck.mutex.Unlock()
	sck.connected = value
}

func (sck *Socket) GetConnect() bool {
	sck.mutex.Lock()
	defer sck.mutex.Unlock()
	return sck.connected
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

func (client *Client) HandleWrite(buffer []byte, response *Response) error {

	result := client.socket.writeAndFlush(buffer)
	resultCode := WaitCodeWithDefaultTimeOut(response)
	/// we need to remove the response before evaluate the
	// buffer errSocket
	result = client.responses.RemoveById(response.subId)
	if result != nil {
		// we just log
		fmt.Printf("Error HandleWrite %s", result)
	}

	return resultCode
}
