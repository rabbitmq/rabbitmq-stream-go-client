package stream

import (
	"bufio"
	"bytes"
	"net"
	"net/url"
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
	CommandPublishConfirm   = 1
	CommandDeclarePublisher = 18
	CommandSaslHandshake    = 9  //2
	CommandTune             = 11 //3
)

func (client *Client) Connect(addr string) error {

	u, err := url.Parse(addr)
	if err != nil {
		return err
	}
	host, port := u.Hostname(), u.Port()
	if port == "" {
		port = "5555"
	}

	client.tuneState.requestedHeartbeat = 60
	client.tuneState.requestedMaxFrameSize = 1048576
	client.clientProperties.items = make(map[string]string)
	connection, err2 := net.Dial("tcp", net.JoinHostPort(host, port))
	if err2 != nil {
		return err2
	}
	client.socket = connection
	client.writer = bufio.NewWriter(client.socket)
	client.reader = bufio.NewReader(client.socket)

	err2 = client.peerProperties()
	if err2 != nil {
		return err2
	}
	pwd, _ := u.User.Password()
	err2 = client.authenticate(u.User.Username(), pwd)
	if err2 != nil {
		return err2
	}
	vhost := "/"
	if len(u.Path) > 1 {
		vhost, _ = url.QueryUnescape(u.Path[1:])
	}
	client.open(vhost)

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

	return client.writeAndFlush(b.Bytes())

}
