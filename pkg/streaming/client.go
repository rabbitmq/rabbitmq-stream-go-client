package streaming

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

type Client struct {
	socket           Socket
	clientProperties ClientProperties
	tuneState        TuneState
	producers        *Producers
	responses        *Responses
	consumers        *Consumers
}

func (client *Client) connect(addr string) error {
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
	client.socket = Socket{connection: connection, mutex: &sync.Mutex{},
		writer: bufio.NewWriter(connection)}
	client.socket.SetConnect(true)

	go client.handleResponse()
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
	client.HeartBeat()

	return nil
}

func (client *Client) peerProperties() error {
	clientPropertiesSize := 4 // size of the map, always there

	client.clientProperties.items["connection_name"] = "rabbitmq-StreamCreator-locator"
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
	correlationId := resp.subId
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

	return client.HandleWrite(b.Bytes(), resp)
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
	correlationId := resp.subId
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	WriteInt(b, length)
	WriteShort(b, CommandSaslHandshake)
	WriteShort(b, Version1)
	WriteInt(b, correlationId)
	err := client.socket.writeAndFlush(b.Bytes())
	data := <-resp.data
	err = client.responses.RemoveById(correlationId)
	if err != nil {
		return nil
	}
	return data.([]string)

}

func (client *Client) sendSaslAuthenticate(saslMechanism string, challengeResponse []byte) error {
	length := 2 + 2 + 4 + 2 + len(saslMechanism) + 4 + len(challengeResponse)
	resp := client.responses.New()
	respTune := client.responses.NewWitName("tune")
	correlationId := resp.subId
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	WriteInt(b, length)
	WriteShort(b, CommandSaslAuthenticate)
	WriteShort(b, Version1)
	WriteInt(b, correlationId)
	WriteString(b, saslMechanism)
	WriteInt(b, len(challengeResponse))
	b.Write(challengeResponse)
	err := client.HandleWrite(b.Bytes(), resp)
	if err != nil {
		return err
	}
	// double read for TUNE
	tuneData := <-respTune.data
	err = client.responses.RemoveByName("tune")
	if err != nil {
		return err
	}

	return client.socket.writeAndFlush(tuneData.([]byte))
}

func (client *Client) open(virtualHost string) error {
	length := 2 + 2 + 4 + 2 + len(virtualHost)
	resp := client.responses.New()
	correlationId := resp.subId
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	WriteInt(b, length)
	WriteShort(b, CommandOpen)
	WriteShort(b, Version1)
	WriteInt(b, correlationId)
	WriteString(b, virtualHost)
	return client.HandleWrite(b.Bytes(), resp)
}

func (client *Client) DeleteStream(stream string) error {
	length := 2 + 2 + 4 + 2 + len(stream)
	resp := client.responses.New()
	correlationId := resp.subId
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	WriteInt(b, length)
	WriteShort(b, CommandDeleteStream)
	WriteShort(b, Version1)
	WriteInt(b, correlationId)
	WriteString(b, stream)

	return client.HandleWrite(b.Bytes(), resp)
}

func (client *Client) UnSubscribe(id uint8) error {
	length := 2 + 2 + 4 + 1
	resp := client.responses.New()
	correlationId := resp.subId
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	WriteInt(b, length)
	WriteShort(b, CommandUnsubscribe)
	WriteShort(b, Version1)
	WriteInt(b, correlationId)
	WriteByte(b, id)
	err := client.HandleWrite(b.Bytes(), resp)

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

	client.socket.writeAndFlush(b.Bytes())
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
