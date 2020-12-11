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
}

const (
	CommandCreateStream     = 998
	Version0                = 0
	CommandPeerProperties   = 15
	UnicodeNull             = "\u0000"
	CommandSaslAuthenticate = 10
	CommandOpen             = 12
	CommandPublish          = 0
	CommandDeclarePublisher = 18
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

	_, _ = client.socket.Write(b.Bytes())

	return nil
}

func (client *Client) peerProperties() {
	clientPropertiesSize := 4 // size of the map, always there

	client.clientProperties.items["connection_name"] = "rabbitmq-stream-locator"
	client.clientProperties.items["product"] = "RabbitMQ Stream"
	client.clientProperties.items["copyright"] = "Copyright (c) 2020 VMware, Inc. or its affiliates."
	client.clientProperties.items["information"] = "Licensed under the MPL 2.0. See https://www.rabbitmq.com/"
	client.clientProperties.items["version"] = "0.1.0"
	client.clientProperties.items["platform"] = "Golang"
	for key, element := range client.clientProperties.items {
		clientPropertiesSize = clientPropertiesSize + 2 + len(key) + 2 + len(element)
	}

	length := 2 + 2 + 4 + clientPropertiesSize

	correlationId := 2
	var b = bytes.NewBuffer(make([]byte, 0, length+4))

	WriteInt(b, length)
	WriteShort(b, CommandPeerProperties)
	WriteShort(b, Version0)
	WriteInt(b, correlationId)
	WriteInt(b, len(client.clientProperties.items))

	for key, element := range client.clientProperties.items {
		WriteString(b, key)
		WriteString(b, element)
	}

	_, _ = client.socket.Write(b.Bytes())
	readerResponse := ReaderResponse{}
	readerResponse.SocketReader = bufio.NewReader(client.socket)
	readerResponse.handleResponse()
}

func (client *Client) authenticate() {
	saslMechanism := "PLAIN"
	response := UnicodeNull + "guest" + UnicodeNull + "guest"
	saslResponse := []byte(response)
	client.sendSaslAuthenticate(saslMechanism, saslResponse)
}

func (client *Client) sendSaslAuthenticate(saslMechanism string, challengeResponse []byte) {
	length := 2 + 2 + 4 + 2 + len(saslMechanism) + 4 + len(challengeResponse)
	fmt.Print(length)
	correlationId := 4
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	WriteInt(b, length)
	WriteShort(b, CommandSaslAuthenticate)
	WriteShort(b, Version0)
	WriteInt(b, correlationId)
	WriteString(b, saslMechanism)
	WriteInt(b, len(challengeResponse))
	b.Write(challengeResponse)
	_, _ = client.socket.Write(b.Bytes())
	readerResponse := ReaderResponse{}
	readerResponse.SocketReader = bufio.NewReader(client.socket)
	readerResponse.handleResponse()
}

func (client *Client) open(virtualHost string) {
	length := 2 + 2 + 4 + 2 + len(virtualHost)
	correlationId := 6
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	WriteInt(b, length)
	WriteShort(b, CommandOpen)
	WriteShort(b, Version0)
	WriteInt(b, correlationId)
	WriteString(b, virtualHost)
	client.socket.Write(b.Bytes())
}

func (client *Client) DeclarePublisher(publisherId byte, stream string) {
	publisherReferenceSize := 0
	length := 2 + 2 + 4 + 1 + 2 + publisherReferenceSize + 2 + len(stream)
	correlationId := 6
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	WriteInt(b, length)
	WriteShort(b, CommandDeclarePublisher)
	WriteShort(b, Version0)
	WriteInt(b, correlationId)
	WriteByte(b, publisherId)
	WriteShort(b, int16(publisherReferenceSize))
	WriteString(b, stream)
	client.socket.Write(b.Bytes())
	var buff = make([]byte, 8)
	client.socket.Read(buff)
	//data := binary.BigEndian.Uint32(buff)
	fmt.Printf("aaa %s", string(buff))
}

func (client *Client) Publish(message string) {
	length := 2 + 2 + 4 + 2 + len(message)
	plublishId := 6
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	WriteInt(b, length)
	WriteShort(b, CommandPublish)
	WriteShort(b, Version0)
	WriteInt(b, plublishId)
	WriteInt(b, 1)
	WriteLong(b, 0)
	WriteString(b, message)
	_, err := client.socket.Write(b.Bytes())
	if err != nil {
		fmt.Printf("%s", err)
	}

	var buff = make([]byte, 10)
	client.socket.Read(buff)

	fmt.Printf("aaa %s", string(buff))
}
