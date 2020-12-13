package stream

import (
	"bufio"
	"bytes"
	"fmt"
	"net"
)

type TuneState struct {
	requestedMaxFrameSize int
	requestedHeartbeat    int
}

type ClientProperties struct {
	items map[string]string
}

type Client struct {
	socket           net.Conn
	clientProperties ClientProperties
	tuneState        TuneState
	writer           *bufio.Writer
	reader           *bufio.Reader
}

const (
	CommandCreateStream     = 998
	Version0                = 0
	CommandPeerProperties   = 15 //1
	UnicodeNull             = "\u0000"
	CommandSaslAuthenticate = 10 //3
	CommandOpen             = 12
	CommandPublish          = 0
	CommandDeclarePublisher = 18
	CommandSaslHandshake    = 9  //2
	CommandTune             = 11 //3
)

func (client *Client) Create() error {
	client.tuneState.requestedHeartbeat = 60
	client.tuneState.requestedMaxFrameSize = 1048576
	client.clientProperties.items = make(map[string]string)
	connection, err2 := net.Dial("tcp", "localhost:5555")
	if err2 != nil {
		fmt.Println(err2)
	}
	client.socket = connection
	client.writer = bufio.NewWriter(client.socket)
	client.reader = bufio.NewReader(client.socket)

	client.peerProperties()
	client.authenticate()
	client.open("/")

	return nil
}

func (client *Client) CreateStream(stream string) error {
	length := 2 + 2 + 4 + 2 + len(stream) + 4
	correlationId := 0
	arguments := make(map[string]string)
	arguments["queue-leader-locator"] = "least-leaders"
	for key, element := range arguments {
		length = length + 2 + len(key) + 2 + len(element)
	}

	var b = bytes.NewBuffer(make([]byte, 0, length))

	WriteInt(b, length)

	WriteShort(b, CommandCreateStream)
	WriteShort(b, Version0)

	WriteInt(b, correlationId)
	WriteString(b, stream)
	WriteInt(b, len(arguments))

	for key, element := range arguments {
		WriteString(b, key)
		WriteString(b, element)
	}

	client.writeAndFlush(b.Bytes())

	return nil
}

