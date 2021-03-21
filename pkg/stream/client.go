package stream

import (
	"bufio"
	"bytes"
	"fmt"
	"net"
	"net/url"
	"sync"
	"time"
)

type TuneState struct {
	requestedMaxFrameSize int
	requestedHeartbeat    int
}

type ClientProperties struct {
	items map[string]string
}

type Socket struct {
	connection net.Conn
	connected  bool
	mutex      *sync.Mutex
}

func (socket *Socket) SetConnect(value bool) {
	socket.mutex.Lock()
	defer socket.mutex.Unlock()
	socket.connected = value
}

func (socket *Socket) GetConnect() bool {
	socket.mutex.Lock()
	defer socket.mutex.Unlock()
	return socket.connected
}

type Client struct {
	socket           Socket
	clientProperties ClientProperties
	tuneState        TuneState
	writer           *bufio.Writer
	reader           *bufio.Reader
	producers        *Producers
	responses        *Responses
	consumers        *Consumers
}

func NewStreamingClient() *Client {
	client := &Client{
		producers: NewProducers(),
		responses: NewResponses(),
		consumers: NewConsumers(),
	}
	return client
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
	client.socket = struct {
		connection net.Conn
		connected  bool
		mutex      *sync.Mutex
	}{connection: connection, mutex: &sync.Mutex{}}

	client.writer = bufio.NewWriter(client.socket.connection)
	client.socket.SetConnect(true)

	go client.handleResponse()
	_, err2 = client.peerProperties()

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
	_, err2 = client.open(vhost)
	if err2 != nil {
		return err2
	}
	client.HeartBeat()

	return nil
}

func (client *Client) CreateStream(stream string) (Code, error) {

	resp := client.responses.New()
	length := 2 + 2 + 4 + 2 + len(stream) + 4
	correlationId := resp.SubId
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

	err := client.writeAndFlush(b.Bytes())
	if err != nil {
		return Code{}, err
	}
	code, err := WaitCodeWithDefaultTimeOut(resp, CommandCreateStream)
	if err != nil {
		return Code{}, err
	}

	err = client.responses.RemoveById(correlationId)
	if err != nil {
		return Code{}, err
	}
	return code, nil
}

func (client *Client) peerProperties() (Code, error) {
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
	resp := client.responses.New()
	correlationId := resp.SubId
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
		return Code{}, err
	}
	code, err := WaitCodeWithDefaultTimeOut(resp, CommandPeerProperties)
	if err != nil {
		return Code{}, err
	}
	err = client.responses.RemoveById(correlationId)
	if err != nil {
		return Code{}, err
	}
	return code, nil
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
	resp := client.responses.New()
	correlationId := resp.SubId
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	WriteInt(b, length)
	WriteShort(b, CommandSaslHandshake)
	WriteShort(b, Version1)
	WriteInt(b, correlationId)
	client.writeAndFlush(b.Bytes())
	data := <-resp.data
	err := client.responses.RemoveById(correlationId)
	if err != nil {
		return nil
	}
	return data.([]string)

}

func (client *Client) sendSaslAuthenticate(saslMechanism string, challengeResponse []byte) error {
	length := 2 + 2 + 4 + 2 + len(saslMechanism) + 4 + len(challengeResponse)
	resp := client.responses.New()
	respTune := client.responses.NewWitName("tune")
	correlationId := resp.SubId
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

	_, err = WaitCodeWithDefaultTimeOut(resp, CommandSaslAuthenticate)
	if err != nil {
		return err
	}
	err = client.responses.RemoveById(correlationId)
	if err != nil {
		return err
	}

	// double read for TUNE
	tuneData := <-respTune.data
	err = client.responses.RemoveByName("tune")
	if err != nil {
		return err
	}

	return client.writeAndFlush(tuneData.([]byte))
}

func (client *Client) open(virtualHost string) (Code, error) {
	length := 2 + 2 + 4 + 2 + len(virtualHost)
	resp := client.responses.New()
	correlationId := resp.SubId
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	WriteInt(b, length)
	WriteShort(b, CommandOpen)
	WriteShort(b, Version1)
	WriteInt(b, correlationId)
	WriteString(b, virtualHost)
	err := client.writeAndFlush(b.Bytes())
	if err != nil {
		return Code{}, err
	}
	code, err := WaitCodeWithDefaultTimeOut(resp, CommandOpen)
	if err != nil {
		return Code{}, err
	}

	err = client.responses.RemoveById(correlationId)
	if err != nil {
		return Code{}, err
	}
	return code, nil
}

func (client *Client) DeleteStream(stream string) (Code, error) {
	length := 2 + 2 + 4 + 2 + len(stream)
	resp := client.responses.New()
	correlationId := resp.SubId
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	WriteInt(b, length)
	WriteShort(b, CommandDeleteStream)
	WriteShort(b, Version1)
	WriteInt(b, correlationId)
	WriteString(b, stream)
	err := client.writeAndFlush(b.Bytes())
	if err != nil {
		return Code{}, err
	}
	code, err := WaitCodeWithDefaultTimeOut(resp, CommandDeleteStream)
	if err != nil {
		return Code{}, err
	}
	err = client.responses.RemoveById(correlationId)
	if err != nil {
		return Code{}, err
	}
	return code, nil
}

func (client *Client) writeAndFlush(buffer []byte) error {
	client.socket.mutex.Lock()
	_, err := client.writer.Write(buffer)
	if err != nil {
		return err
	}
	err = client.writer.Flush()
	if err != nil {
		return err
	}
	client.socket.mutex.Unlock()
	return nil
}

func (client *Client) UnSubscribe(id uint8) error {
	length := 2 + 2 + 4 + 1
	resp := client.responses.New()
	correlationId := resp.SubId
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	WriteInt(b, length)
	WriteShort(b, CommandUnsubscribe)
	WriteShort(b, Version1)
	WriteInt(b, correlationId)
	WriteByte(b, id)
	err := client.writeAndFlush(b.Bytes())
	if err != nil {
		return err
	}
	_, err = WaitCodeWithDefaultTimeOut(resp, CommandUnsubscribe)
	if err != nil {
		return  err
	}
	err = client.responses.RemoveById(correlationId)
	if err != nil {
		return err
	}

	consumer, err := client.consumers.GetById(id)
	if err != nil {
		return err
	}
	consumer.response.code <- Code{id: CloseChannel}
	return nil
}

func (client *Client) HeartBeat() {

	ticker := time.NewTicker(40 * time.Second)
	resp := client.responses.NewWitName("heartbeat")
	go func() {
		for {
			select {
			case <-resp.code:
				client.responses.RemoveByName("heartbeat")
				return
			case t := <-ticker.C:
				fmt.Printf("sendHeartbeat: %s \n", t)
				client.sendHeartbeat()
			}
		}
	}()
}

func (client *Client) sendHeartbeat() {
	length := 4 + 2 + 2
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	WriteInt(b, 4)
	WriteShort(b, CommandHeartbeat)
	WriteShort(b, Version1)

	client.writeAndFlush(b.Bytes())
}

func (client *Client) Close() error {
	client.socket.mutex.Lock()
	defer client.socket.mutex.Unlock()
	if client.socket.connected {
		r, err := client.responses.GetByName("heartbeat")
		if err != nil {
			return err
		}
		r.code <- Code{id: CloseChannel}
		err = client.socket.connection.Close()
		client.socket.connected = false
		return err
	}
	//}
	return nil
}
