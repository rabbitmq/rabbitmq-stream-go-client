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
	err2 = client.open(vhost)
	if err2 != nil {
		return err2
	}
	InitProducersCoordinator()
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

func (client *Client) NewProducer(stream string) (*Producer, error) {
	return client.declarePublisher(stream)
}

func (client *Client) declarePublisher(stream string) (*Producer, error) {
	producer, _ := GetProducersCoordinator().registerNewProducer()

	publisherReferenceSize := 0
	length := 2 + 2 + 4 + 1 + 2 + publisherReferenceSize + 2 + len(stream)
	correlationId := 6
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	WriteInt(b, length)
	WriteShort(b, CommandDeclarePublisher)
	WriteShort(b, Version0)
	WriteInt(b, correlationId)
	WriteByte(b, producer.ProducerID)
	WriteShort(b, int16(publisherReferenceSize))
	WriteString(b, stream)
	err := client.writeAndFlush(b.Bytes())
	if err != nil {
		return nil, err
	}
	client.handleResponse()
	producer.LikedClient = client
	return producer, nil
}

func (client *Client) peerProperties() error {
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

	err := client.writeAndFlush(b.Bytes())
	if err != nil {
		return err
	}
	client.handleResponse()
	return nil
}

func (client *Client) authenticate(user string, password string) error {

	saslMechanisms := client.getSaslMechanisms()
	saslMechanism := ""
	for i := 0; i < len(saslMechanisms); i++ {
		if saslMechanisms[i] == "PLAIN" {
			saslMechanism = "PLAIN"
		}
	}
	response := UnicodeNull + user + UnicodeNull + password
	saslResponse := []byte(response)
	return client.sendSaslAuthenticate(saslMechanism, saslResponse)
}

func (client *Client) getSaslMechanisms() []string {
	length := 2 + 2 + 4
	correlationId := 3
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	WriteInt(b, length)
	WriteShort(b, CommandSaslHandshake)
	WriteShort(b, Version0)
	WriteInt(b, correlationId)
	client.writeAndFlush(b.Bytes())
	data := client.handleResponse()
	strings := data.([]string)
	return strings

}

func (client *Client) sendSaslAuthenticate(saslMechanism string, challengeResponse []byte) error {
	length := 2 + 2 + 4 + 2 + len(saslMechanism) + 4 + len(challengeResponse)
	correlationId := 4
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	WriteInt(b, length)
	WriteShort(b, CommandSaslAuthenticate)
	WriteShort(b, Version0)
	WriteInt(b, correlationId)
	WriteString(b, saslMechanism)
	WriteInt(b, len(challengeResponse))
	b.Write(challengeResponse)
	err := client.writeAndFlush(b.Bytes())
	if err != nil {
		return err
	}

	client.handleResponse()

	// double read for TUNE
	data := client.handleResponse()
	tuneData := data.([]byte)
	return client.writeAndFlush(tuneData)

}

func (client *Client) open(virtualHost string) error {
	length := 2 + 2 + 4 + 2 + len(virtualHost)
	correlationId := 6
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	WriteInt(b, length)
	WriteShort(b, CommandOpen)
	WriteShort(b, Version0)
	WriteInt(b, correlationId)
	WriteString(b, virtualHost)
	err := client.writeAndFlush(b.Bytes())
	if err != nil {
		return err
	}
	client.handleResponse()
	return nil
}

func (client *Client) writeAndFlush(buffer []byte) error {

	_, err := client.writer.Write(buffer)
	if err != nil {
		return err
	}
	err = client.writer.Flush()
	if err != nil {
		return err
	}
	return nil
}
