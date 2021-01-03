package stream

import (
	"bytes"
)

func (client *Client) handleResponse() interface{} {
	response := &StreamingResponse{}
	response.FrameLen = ReadIntFromReader(client.reader)
	response.CommandID = ReadShortFromReader(client.reader)
	response.Version = ReadShortFromReader(client.reader)
	defer client.reader.Reset(client.socket)

	switch response.CommandID {

	case CommandPeerProperties:
		{
			return client.handlePeerProperties(response)
		}
	case CommandSaslHandshake:
		{
			return client.handleSaslHandshakeResponse(response)
		}
	case CommandTune:
		{
			return client.handleTune()
		}
	case CommandOpen, CommandDeclarePublisher:
		{
			return client.handleGenericResponse(response)
		}

	case CommandPublishConfirm:
		{
			return client.handleConfirm(response)
		}

	}

	return nil
}

func (client *Client) handleSaslHandshakeResponse(response *StreamingResponse) interface{} {
	response.CorrelationId = ReadIntFromReader(client.reader)
	response.ResponseCode = ReadShortFromReader(client.reader)
	mechanismsCount := ReadIntFromReader(client.reader)
	var mechanisms []string
	for i := 0; i < int(mechanismsCount); i++ {
		mechanism := ReadStringFromReader(client.reader)
		mechanisms = append(mechanisms, mechanism)
	}
	return mechanisms
}

func (client *Client) handlePeerProperties(response *StreamingResponse) interface{} {
	response.CorrelationId = ReadIntFromReader(client.reader)
	response.ResponseCode = ReadShortFromReader(client.reader)

	serverPropertiesCount := ReadIntFromReader(client.reader)
	serverProperties := make(map[string]string)

	for i := 0; i < int(serverPropertiesCount); i++ {
		key := ReadStringFromReader(client.reader)
		value := ReadStringFromReader(client.reader)
		serverProperties[key] = value
	}

	return serverProperties
}

func (client *Client) handleTune() interface{} {

	serverMaxFrameSize := ReadIntFromReader(client.reader)
	serverHeartbeat := ReadIntFromReader(client.reader)

	maxFrameSize := serverMaxFrameSize
	heartbeat := serverHeartbeat

	length := 2 + 2 + 4 + 4
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	WriteInt(b, length)
	WriteShort(b, CommandTune)
	WriteShort(b, Version0)
	WriteInt32(b, maxFrameSize)
	WriteInt32(b, heartbeat)
	return b.Bytes()

}

func (client *Client) handleGenericResponse(response *StreamingResponse) interface{} {

	response.ResponseCode = ReadShortFromReader(client.reader)
	response.CorrelationId = ReadIntFromReader(client.reader)
	return response.ResponseCode
}

func (client *Client) handleConfirm(response *StreamingResponse) interface{} {
	response.PublishID = ReadByteFromReader(client.reader)
	//response.PublishingIdCount = ReadIntFromReader(client.reader)
	publishingIdCount := ReadIntFromReader(client.reader)
	//var _publishingId int64
	for publishingIdCount != 0 {
		//publishingId = ReadInt64FromReader(client.reader)
		ReadInt64FromReader(client.reader)
		//fmt.Print("publishingId %d",  publishingId)
		publishingIdCount--
	}
	return 0

}
