package stream

import (
	"bufio"
	"bytes"
	"net"
	"net/url"
	"sync"
)

type TuneState struct {
	requestedMaxFrameSize int
	requestedHeartbeat    int
}

type ClientProperties struct {
	items map[string]string
}

type AtomicInt struct {
	value int
	mutex *sync.Mutex
}

type Client struct {
	socket           net.Conn
	clientProperties ClientProperties
	tuneState        TuneState
	writer           *bufio.Writer
	reader           *bufio.Reader
	correlationID    *AtomicInt
}

const (
	CommandDeclarePublisher       = 1
	CommandPublish                = 2
	CommandPublishConfirm         = 3
	CommandPublishError           = 4
	CommandQueryPublisherSequence = 5
	CommandDeletePublisher        = 6
	CommandSubscribe              = 7
	CommandDeliver                = 8
	CommandCredit                 = 9
	CommandCommitOffset           = 10
	CommandQueryOffset            = 11
	CommandUnsubscribe            = 12
	CommandCreateStream           = 13
	CommandDeleteStream           = 14
	CommandMetadata               = 15
	CommandMetadataUpdate         = 16
	CommandPeerProperties         = 17
	CommandSaslHandshake          = 18
	CommandSaslAuthenticate       = 19
	CommandTune                   = 20
	CommandOpen                   = 21
	CommandClose                  = 22
	CommandHeartbeat              = 23

	Version1    = 1
	UnicodeNull = "\u0000"
)

func NewAtomicInt() *AtomicInt {
	atomicInt := &AtomicInt{}
	atomicInt.value = 0
	atomicInt.mutex = &sync.Mutex{}
	return atomicInt
}

func NewStreamingClient() *Client {
	client := &Client{}
	client.correlationID = NewAtomicInt()
	return client
}

func (client *Client) increaseAndGetCorrelationID() int {
	client.correlationID.mutex.Lock()
	defer client.correlationID.mutex.Unlock()
	client.correlationID.value += 1
	return client.correlationID.value
}

func (client *Client) Connect(addr string) error {

	u, err := url.Parse(addr)
	if err != nil {
		return err
	}
	host, port := u.Hostname(), u.Port()
	if port == "" {
		port = "5551"
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
	correlationId := client.increaseAndGetCorrelationID()
	arguments := make(map[string]string)
	arguments["queue-leader-locator"] = "least-leaders"
	for key, element := range arguments {
		length = length + 2 + len(key) + 2 + len(element)
	}

	var b = bytes.NewBuffer(make([]byte, 0, length))

	WriteInt(b, length)

	WriteShort(b, CommandCreateStream)
	WriteShort(b, Version1)

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
	correlationId := client.increaseAndGetCorrelationID()
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	WriteInt(b, length)
	WriteShort(b, CommandDeclarePublisher)
	WriteShort(b, Version1)
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
	client.clientProperties.items["copyright"] = "Copyright (c) 2021 VMware, Inc. or its affiliates."
	client.clientProperties.items["information"] = "Licensed under the MPL 2.0. See https://www.rabbitmq.com/"
	client.clientProperties.items["version"] = "0.1.0"
	client.clientProperties.items["platform"] = "Golang"
	for key, element := range client.clientProperties.items {
		clientPropertiesSize = clientPropertiesSize + 2 + len(key) + 2 + len(element)
	}

	length := 2 + 2 + 4 + clientPropertiesSize

	correlationId := client.increaseAndGetCorrelationID()
	var b = bytes.NewBuffer(make([]byte, 0, length+4))

	WriteInt(b, length)
	WriteShort(b, CommandPeerProperties)
	WriteShort(b, Version1)
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
	correlationId := client.increaseAndGetCorrelationID()
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	WriteInt(b, length)
	WriteShort(b, CommandSaslHandshake)
	WriteShort(b, Version1)
	WriteInt(b, correlationId)
	client.writeAndFlush(b.Bytes())
	data := client.handleResponse()
	strings := data.([]string)
	return strings

}

func (client *Client) sendSaslAuthenticate(saslMechanism string, challengeResponse []byte) error {
	length := 2 + 2 + 4 + 2 + len(saslMechanism) + 4 + len(challengeResponse)
	correlationId := client.increaseAndGetCorrelationID()
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	WriteInt(b, length)
	WriteShort(b, CommandSaslAuthenticate)
	WriteShort(b, Version1)
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
	correlationId := client.increaseAndGetCorrelationID()
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	WriteInt(b, length)
	WriteShort(b, CommandOpen)
	WriteShort(b, Version1)
	WriteInt(b, correlationId)
	WriteString(b, virtualHost)
	err := client.writeAndFlush(b.Bytes())
	if err != nil {
		return err
	}
	client.handleResponse()
	return nil
}

func (client *Client) deletePublisher(publisherId byte) error {
	length := 2 + 2 + 4 + 1
	correlationId := client.increaseAndGetCorrelationID()
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	WriteInt(b, length)
	WriteShort(b, CommandDeletePublisher)
	WriteShort(b, Version1)
	WriteInt(b, correlationId)
	WriteByte(b, publisherId)
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
