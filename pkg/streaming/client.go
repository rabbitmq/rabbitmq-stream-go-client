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

func (c *Client) connect(addr string) error {
	u, err := url.Parse(addr)
	if err != nil {
		return err
	}
	host, port := u.Hostname(), u.Port()
	if port == "" {
		port = "5551"
	}

	c.tuneState.requestedHeartbeat = 60
	c.tuneState.requestedMaxFrameSize = 1048576
	c.clientProperties.items = make(map[string]string)
	connection, err2 := net.Dial("tcp", net.JoinHostPort(host, port))
	if err2 != nil {
		return err2
	}
	c.socket = Socket{connection: connection, mutex: &sync.Mutex{},
		writer: bufio.NewWriter(connection)}
	c.socket.SetConnect(true)

	go c.handleResponse()
	err2 = c.peerProperties()

	if err2 != nil {
		return err2
	}
	pwd, _ := u.User.Password()
	err2 = c.authenticate(u.User.Username(), pwd)
	if err2 != nil {
		return err2
	}
	vhost := "/"
	if len(u.Path) > 1 {
		vhost, _ = url.QueryUnescape(u.Path[1:])
	}
	err2 = c.open(vhost)
	if err2 != nil {
		return err2
	}
	c.HeartBeat()

	return nil
}

func (c *Client) peerProperties() error {
	clientPropertiesSize := 4 // size of the map, always there

	c.clientProperties.items["connection_name"] = "rabbitmq-StreamCreator-locator"
	c.clientProperties.items["product"] = "RabbitMQ Stream"
	c.clientProperties.items["copyright"] = "Copyright (c) 2021 VMware, Inc. or its affiliates."
	c.clientProperties.items["information"] = "Licensed under the MPL 2.0. See https://www.rabbitmq.com/"
	c.clientProperties.items["version"] = "0.1.0"
	c.clientProperties.items["platform"] = "Golang"
	for key, element := range c.clientProperties.items {
		clientPropertiesSize = clientPropertiesSize + 2 + len(key) + 2 + len(element)
	}

	length := 2 + 2 + 4 + clientPropertiesSize
	resp := c.responses.New()
	correlationId := resp.subId
	var b = bytes.NewBuffer(make([]byte, 0, length+4))

	WriteInt(b, length)
	WriteShort(b, CommandPeerProperties)
	WriteShort(b, Version1)
	WriteInt(b, correlationId)
	WriteInt(b, len(c.clientProperties.items))

	for key, element := range c.clientProperties.items {
		WriteString(b, key)
		WriteString(b, element)
	}

	return c.HandleWrite(b.Bytes(), resp)
}

func (c *Client) authenticate(user string, password string) error {

	saslMechanisms := c.getSaslMechanisms()
	saslMechanism := ""
	for i := 0; i < len(saslMechanisms); i++ {
		if saslMechanisms[i] == "PLAIN" {
			saslMechanism = "PLAIN"
		}
	}
	response := UnicodeNull + user + UnicodeNull + password
	saslResponse := []byte(response)
	return c.sendSaslAuthenticate(saslMechanism, saslResponse)
}

func (c *Client) getSaslMechanisms() []string {
	length := 2 + 2 + 4
	resp := c.responses.New()
	correlationId := resp.subId
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	WriteInt(b, length)
	WriteShort(b, CommandSaslHandshake)
	WriteShort(b, Version1)
	WriteInt(b, correlationId)
	err := c.socket.writeAndFlush(b.Bytes())
	data := <-resp.data
	err = c.responses.RemoveById(correlationId)
	if err != nil {
		return nil
	}
	return data.([]string)

}

func (c *Client) sendSaslAuthenticate(saslMechanism string, challengeResponse []byte) error {
	length := 2 + 2 + 4 + 2 + len(saslMechanism) + 4 + len(challengeResponse)
	resp := c.responses.New()
	respTune := c.responses.NewWitName("tune")
	correlationId := resp.subId
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	WriteInt(b, length)
	WriteShort(b, CommandSaslAuthenticate)
	WriteShort(b, Version1)
	WriteInt(b, correlationId)
	WriteString(b, saslMechanism)
	WriteInt(b, len(challengeResponse))
	b.Write(challengeResponse)
	err := c.HandleWrite(b.Bytes(), resp)
	if err != nil {
		return err
	}
	// double read for TUNE
	tuneData := <-respTune.data
	err = c.responses.RemoveByName("tune")
	if err != nil {
		return err
	}

	return c.socket.writeAndFlush(tuneData.([]byte))
}

func (c *Client) open(virtualHost string) error {
	length := 2 + 2 + 4 + 2 + len(virtualHost)
	resp := c.responses.New()
	correlationId := resp.subId
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	WriteInt(b, length)
	WriteShort(b, CommandOpen)
	WriteShort(b, Version1)
	WriteInt(b, correlationId)
	WriteString(b, virtualHost)
	return c.HandleWrite(b.Bytes(), resp)
}

func (c *Client) DeleteStream(stream string) error {
	length := 2 + 2 + 4 + 2 + len(stream)
	resp := c.responses.New()
	correlationId := resp.subId
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	WriteInt(b, length)
	WriteShort(b, CommandDeleteStream)
	WriteShort(b, Version1)
	WriteInt(b, correlationId)
	WriteString(b, stream)

	return c.HandleWrite(b.Bytes(), resp)
}

func (c *Client) UnSubscribe(id uint8) error {
	length := 2 + 2 + 4 + 1
	resp := c.responses.New()
	correlationId := resp.subId
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	WriteInt(b, length)
	WriteShort(b, CommandUnsubscribe)
	WriteShort(b, Version1)
	WriteInt(b, correlationId)
	WriteByte(b, id)
	err := c.HandleWrite(b.Bytes(), resp)

	consumer, err := c.consumers.GetById(id)
	if err != nil {
		return err
	}
	consumer.response.code <- Code{id: CloseChannel}
	return nil
}

func (c *Client) HeartBeat() {

	ticker := time.NewTicker(40 * time.Second)
	resp := c.responses.NewWitName("heartbeat")
	go func() {
		for {
			select {
			case <-resp.code:
				c.responses.RemoveByName("heartbeat")
				return
			case t := <-ticker.C:
				fmt.Printf("sendHeartbeat: %s \n", t)
				c.sendHeartbeat()
			}
		}
	}()
}

func (c *Client) sendHeartbeat() {
	length := 4 + 2 + 2
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	WriteInt(b, 4)
	WriteShort(b, CommandHeartbeat)
	WriteShort(b, Version1)

	c.socket.writeAndFlush(b.Bytes())
}

func (c *Client) Close() error {
	c.socket.mutex.Lock()
	defer c.socket.mutex.Unlock()
	if c.socket.connected {
		r, err := c.responses.GetByName("heartbeat")
		if err != nil {
			return err
		}
		r.code <- Code{id: CloseChannel}
		err = c.socket.connection.Close()
		c.socket.connected = false
		return err
	}
	//}
	return nil
}

