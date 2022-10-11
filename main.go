package main

import (
	"bufio"
	"fmt"
	"github.com/gsantomaggio/rabbitmq-stream-go-client/pkg/stream"
	"net"
	"os"
)

func main() {

	servAddr := net.JoinHostPort("localhost", "5552")
	tcpAddr, _ := net.ResolveTCPAddr("tcp", servAddr)
	connection, errorConnection := net.DialTCP("tcp", nil, tcpAddr)
	if errorConnection != nil {
		panic(errorConnection)
	}
	var tc stream.Clienter
	tc = stream.NewTcpClient(connection)
	//time.Sleep(1 * time.Second)
	err := tc.Connect(nil, nil)
	if err != nil {
		return
	}

	fmt.Println("Press any key to stop ")
	reader := bufio.NewReader(os.Stdin)
	_, _ = reader.ReadString('\n')
}
