package stream

import (
	"bufio"
	"bytes"
	"fmt"
	"net"
)

type Response struct {
	isDone     chan bool
	dataString chan []string
	dataBytes  chan []byte
	subId      int
}

func (client *Client) handleResponse(conn net.Conn) {
	r := bufio.NewReader(conn)
	for {
		response := &StreamingResponse{}
		response.FrameLen = ReadUIntFromReader(r)
		response.CommandID = UShortExtractResponseCode(ReadUShortFromReader(r))
		response.Version = ReadUShortFromReader(r)

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

func (client *Client) handleSaslHandshakeResponse(streamingRes *StreamingResponse, r *bufio.Reader) interface{} {
	streamingRes.CorrelationId = ReadUIntFromReader(r)
	streamingRes.ResponseCode = UShortExtractResponseCode(ReadUShortFromReader(r))
	mechanismsCount := ReadUIntFromReader(r)
	var mechanisms []string
	for i := 0; i < int(mechanismsCount); i++ {
		mechanism := ReadStringFromReader(r)
		mechanisms = append(mechanisms, mechanism)
	}

	res, err := GetResponses().GetById(streamingRes.CorrelationId)
	if err != nil {
		// TODO handle response
		return err
	}
	res.dataString <- mechanisms

	return mechanisms
}

func (client *Client) handlePeerProperties(streamingRes *StreamingResponse, r *bufio.Reader) interface{} {
	streamingRes.CorrelationId = ReadUIntFromReader(r)
	streamingRes.ResponseCode = UShortExtractResponseCode(ReadUShortFromReader(r))
	if streamingRes.ResponseCode != 1 {
		fmt.Printf("Errr ResponseCode: %d ", streamingRes.ResponseCode)
	}
	serverPropertiesCount := ReadUIntFromReader(r)
	serverProperties := make(map[string]string)

	for i := 0; i < int(serverPropertiesCount); i++ {
		key := ReadStringFromReader(r)
		value := ReadStringFromReader(r)
		serverProperties[key] = value
	}
	res, err := GetResponses().GetById(streamingRes.CorrelationId)
	if err != nil {
		// TODO handle response
		return err
	}
	res.isDone <- true
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
	res, err := GetResponses().GetByName("tune")
	if err != nil {
		// TODO handle response
		return err
	}
	res.dataBytes <- b.Bytes()
	return b.Bytes()

}

func (client *Client) handleGenericResponse(response *StreamingResponse, r *bufio.Reader) interface{} {
	response.CorrelationId = ReadUIntFromReader(r)
	response.ResponseCode = UShortExtractResponseCode(ReadUShortFromReader(r))
	if response.ResponseCode != 1 {
		fmt.Printf("Errr ResponseCode: %d \n", response.ResponseCode)

	}
	res, err := GetResponses().GetById(response.CorrelationId)
	if err != nil {
		// TODO handle response
		return err
	}
	res.isDone <- true
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
	//v := GetProducers().GetById(response.PublishID)
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
