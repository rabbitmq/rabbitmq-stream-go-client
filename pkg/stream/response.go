package stream

import (
	"bytes"
	"fmt"
	"time"
)

func (client *Client) handleResponse() {

	for {
		response := &StreamingResponse{}
		response.FrameLen = ReadIntFromReader(client.reader)
		response.CommandID = UShortExtractResponseCode(ReadUShortFromReader(client.reader))
		response.Version = ReadShortFromReader(client.reader)

		//defer
		//fmt.Printf("CommandID %d \n", response.CommandID)
		switch response.CommandID {

		case CommandPeerProperties:
			{
				client.handlePeerProperties(response)
			}
		case CommandSaslHandshake:
			{

				client.handleSaslHandshakeResponse(response)
			}
		case CommandTune:
			{
				client.handleTune(response)
			}
		case CommandOpen, CommandDeclarePublisher,
			CommandDeletePublisher, CommandDeleteStream,
			CommandCreateStream, CommandSaslAuthenticate:
			{
				client.handleGenericResponse(response)
			}

		case CommandPublishConfirm:
			{
				client.handleConfirm(response)
			}

		}


	}

}

func (client *Client) handleSaslHandshakeResponse(response *StreamingResponse) interface{} {
	response.CorrelationId = ReadIntFromReader(client.reader)
	response.ResponseCode = UShortExtractResponseCode(ReadUShortFromReader(client.reader))
	mechanismsCount := ReadIntFromReader(client.reader)
	var mechanisms []string
	for i := 0; i < int(mechanismsCount); i++ {
		mechanism := ReadStringFromReader(client.reader)
		mechanisms = append(mechanisms, mechanism)
	}

	GetResponses().GetResponderById(response.CorrelationId).dataString <- mechanisms
	return mechanisms
}

func (client *Client) handlePeerProperties(response *StreamingResponse) interface{} {
	response.CorrelationId = ReadIntFromReader(client.reader)
	response.ResponseCode = UShortExtractResponseCode(ReadUShortFromReader(client.reader))
	if response.ResponseCode != 1 {
		fmt.Printf("Errr ResponseCode: %d ", response.ResponseCode)
	}
	serverPropertiesCount := ReadIntFromReader(client.reader)
	serverProperties := make(map[string]string)

	for i := 0; i < int(serverPropertiesCount); i++ {
		key := ReadStringFromReader(client.reader)
		value := ReadStringFromReader(client.reader)
		serverProperties[key] = value
	}
	GetResponses().GetResponderById(response.CorrelationId).isDone <- true
	return serverProperties
}

func (client *Client) handleTune(response *StreamingResponse) interface{} {

	serverMaxFrameSize := ReadIntFromReader(client.reader)
	serverHeartbeat := ReadIntFromReader(client.reader)

	maxFrameSize := serverMaxFrameSize
	heartbeat := serverHeartbeat

	length := 2 + 2 + 4 + 4
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	WriteInt(b, length)
	WriteUShort(b, UShortEncodeResponseCode(CommandTune))
	WriteShort(b, Version1)
	WriteInt32(b, maxFrameSize)
	WriteInt32(b, heartbeat)
	GetResponses().GetResponderByName("tune").dataBytes <- b.Bytes()
	return b.Bytes()

}

func (client *Client) handleGenericResponse(response *StreamingResponse) interface{} {
	response.CorrelationId = ReadIntFromReader(client.reader)
	response.ResponseCode = UShortExtractResponseCode(ReadUShortFromReader(client.reader))
	if response.ResponseCode != 1 {
		fmt.Printf("Errr ResponseCode: %d \n", response.ResponseCode)

	}
	var r = GetResponses().GetResponderById(response.CorrelationId)
	if r != nil {
		r.isDone <- true
	} else {
		println("Errr")
	}

	return response.ResponseCode
}

func (client *Client) handleConfirm(response *StreamingResponse) interface{} {
	response.PublishID = ReadByteFromReader(client.reader)
	//response.PublishingIdCount = ReadIntFromReader(client.reader)
	publishingIdCount := ReadIntFromReader(client.reader)
	//var _publishingId int64
	for publishingIdCount != 0 {
		ReadInt64FromReader(client.reader)
		publishingIdCount--
	}

	fmt.Printf("publishedid before: %d   \n", response.PublishID)

	v := GetProducers().GetProducerById(response.PublishID)
	if v != nil {
		select {
		case v.PublishConfirm.isDone <- true:
			//return 0, nil
		case <-time.After(200 * time.Millisecond):
			//fmt.Printf("timeout id:%d \n", producer.ProducerID)
		}
	} else {
		fmt.Printf("niiillllllll publishedid before %d \n", response.PublishID)
	}

	fmt.Printf("publishedid after: %d \n", response.PublishID)
	return 0

}
