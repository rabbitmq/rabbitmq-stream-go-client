package stream

import (
	"bufio"
	"bytes"
	"fmt"
	"net"
)

func (client *Client) handleResponse(conn net.Conn) {
	r := bufio.NewReader(conn)
	for {
		response := &StreamingResponse{}
		response.FrameLen = ReadUIntFromReader(r)
		response.CommandID = UShortExtractResponseCode(ReadUShortFromReader(r))
		response.Version = ReadUShortFromReader(r)

		//defer
		//fmt.Printf(" CommandID %d buff:%d \n", response.CommandID, r.Buffered())
		switch response.CommandID {

		case CommandPeerProperties:
			{
				client.handlePeerProperties(response, r)
			}
		case CommandSaslHandshake:
			{

				client.handleSaslHandshakeResponse(response, r)
			}
		case CommandTune:
			{
				client.handleTune(response, r)
			}
		case CommandOpen, CommandDeclarePublisher,
			CommandDeletePublisher, CommandDeleteStream,
			CommandCreateStream, CommandSaslAuthenticate:
			{
				client.handleGenericResponse(response, r)
			}

		case CommandPublishConfirm:
			{
				client.handleConfirm(response, r)
			}
		default:
			{
				fmt.Printf("dont CommandID %d buff:%d \n", response.CommandID, r.Buffered())
			}

		}
		//r = bufio.NewReader(conn)

	}

}

func (client *Client) handleSaslHandshakeResponse(response *StreamingResponse, r *bufio.Reader) interface{} {
	response.CorrelationId = ReadUIntFromReader(r)
	response.ResponseCode = UShortExtractResponseCode(ReadUShortFromReader(r))
	mechanismsCount := ReadUIntFromReader(r)
	var mechanisms []string
	for i := 0; i < int(mechanismsCount); i++ {
		mechanism := ReadStringFromReader(r)
		mechanisms = append(mechanisms, mechanism)
	}

	GetResponses().GetResponderById(response.CorrelationId).dataString <- mechanisms
	return mechanisms
}

func (client *Client) handlePeerProperties(response *StreamingResponse, r *bufio.Reader) interface{} {
	response.CorrelationId = ReadUIntFromReader(r)
	response.ResponseCode = UShortExtractResponseCode(ReadUShortFromReader(r))
	if response.ResponseCode != 1 {
		fmt.Printf("Errr ResponseCode: %d ", response.ResponseCode)
	}
	serverPropertiesCount := ReadUIntFromReader(r)
	serverProperties := make(map[string]string)

	for i := 0; i < int(serverPropertiesCount); i++ {
		key := ReadStringFromReader(r)
		value := ReadStringFromReader(r)
		serverProperties[key] = value
	}
	GetResponses().GetResponderById(response.CorrelationId).isDone <- true
	return serverProperties
}

func (client *Client) handleTune(response *StreamingResponse, r *bufio.Reader) interface{} {

	serverMaxFrameSize := ReadUIntFromReader(r)
	serverHeartbeat := ReadUIntFromReader(r)

	maxFrameSize := serverMaxFrameSize
	heartbeat := serverHeartbeat

	length := 2 + 2 + 4 + 4
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	WriteInt(b, length)
	WriteUShort(b, UShortEncodeResponseCode(CommandTune))
	WriteShort(b, Version1)
	WriteUInt(b, maxFrameSize)
	WriteUInt(b, heartbeat)
	GetResponses().GetResponderByName("tune").dataBytes <- b.Bytes()
	return b.Bytes()

}

func (client *Client) handleGenericResponse(response *StreamingResponse, r *bufio.Reader) interface{} {
	response.CorrelationId = ReadUIntFromReader(r)
	response.ResponseCode = UShortExtractResponseCode(ReadUShortFromReader(r))
	if response.ResponseCode != 1 {
		fmt.Printf("Errr ResponseCode: %d \n", response.ResponseCode)

	}
	var res = GetResponses().GetResponderById(response.CorrelationId)
	if res != nil {
		res.isDone <- true
	} else {
		println("Errr")
	}

	return response.ResponseCode
}

func (client *Client) handleConfirm(response *StreamingResponse, r *bufio.Reader) interface{} {
	response.PublishID = ReadByteFromReader(r)
	//response.PublishingIdCount = ReadIntFromReader(client.reader)
	publishingIdCount := ReadUIntFromReader(r)
	//var _publishingId int64
	for publishingIdCount != 0 {
		ReadInt64FromReader(r)
		publishingIdCount--
	}

	//fmt.Printf("publishedid before: %d   \n", response.PublishID)
	//
	//v := GetProducers().GetProducerById(response.PublishID)
	//if v != nil {
	//	select {
	//	case v.PublishConfirm.isDone <- true:
	//		//return 0, nil
	//	case <-time.After(200 * time.Millisecond):
	//		//fmt.Printf("timeout id:%d \n", producer.ProducerID)
	//	}
	//} else {
	//	fmt.Printf("niiillllllll publishedid before %d \n", response.PublishID)
	//}
	//
	//fmt.Printf("publishedid after: %d \n", response.PublishID)
	return 0

}
