package stream

import (
	"bytes"
	"fmt"
)

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

	client.writeAndFlush(b.Bytes())
	client.handleResponse()
}

func (client *Client) authenticate() {

	saslMechanisms := client.getSaslMechanisms()
	saslMechanism := ""
	for i := 0; i < len(saslMechanisms); i++ {
		if saslMechanisms[i] == "PLAIN" {
			saslMechanism = "PLAIN"
		}
	}
	response := UnicodeNull + "guest" + UnicodeNull + "guest"
	saslResponse := []byte(response)
	client.sendSaslAuthenticate(saslMechanism, saslResponse)
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
	client.writeAndFlush(b.Bytes())

	client.handleResponse()

	// double read for TUNE
	data := client.handleResponse()
	tuneData := data.([]byte)
	client.writeAndFlush(tuneData)

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
	client.writeAndFlush(b.Bytes())
	client.handleResponse()
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
	client.writeAndFlush(b.Bytes())
	client.handleResponse()

}

func (client *Client) Publish(message string) {
	length := 30
	var publishId byte
	publishId = 0
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	WriteInt(b, length)
	WriteShort(b, CommandPublish)
	WriteShort(b, Version0)
	WriteByte(b, publishId)
	WriteInt(b, 1)
	WriteLong(b, 0)
	WriteInt(b, 9)

	WriteByte(b, 0)
	WriteByte(b, 83)
	WriteByte(b, 117)
	WriteByte(b, 96)
	WriteByte(b, 4)
	WriteByte(b, 90)
	WriteByte(b, 90)
	WriteByte(b, 90)
	WriteByte(b, 90)


	//bb := []byte{0, 83, 117, byte(-96), 4, 90, 90, 90, 90}
	//b.Write(bb)
	client.writeAndFlush(b.Bytes())
	//client.handleResponse()
}