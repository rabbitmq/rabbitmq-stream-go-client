package stream

import (
	"bytes"
	"fmt"
	"io"
)

type ReaderResponse struct {
	SocketReader io.Reader
	response     *Response
}

func (readerResponse ReaderResponse) handleResponse() interface{} {
	readerResponse.response = &Response{}
	readerResponse.response.ReadResponseFromStream(readerResponse.SocketReader)
	fmt.Printf("number:%d, \n", readerResponse.response.CommandID)
	switch readerResponse.response.CommandID {

	case CommandPeerProperties:
		{
			return readerResponse.handlePeerProperties()
		}
	case CommandSaslHandshake:
		{
			return readerResponse.handleSaslHandshakeResponse()
		}
	case CommandTune:
		{
			return readerResponse.handleTune()
		}
	case CommandOpen, CommandDeclarePublisher:
		{
			readerResponse.handleGenericResponse()
		}
	}
	return nil
}

func (readerResponse ReaderResponse) handleSaslHandshakeResponse() interface{} {
	readerResponse.response.CorrelationId = ReadIntFromReader(readerResponse.SocketReader)
	readerResponse.response.ResponseCode = ReadShortFromReader(readerResponse.SocketReader)
	mechanismsCount := ReadIntFromReader(readerResponse.SocketReader)
	var mechanisms []string
	for i := 0; i < int(mechanismsCount); i++ {
		mechanism := ReadStringFromReader(readerResponse.SocketReader)
		mechanisms = append(mechanisms, mechanism)
	}
	return mechanisms
}

func (readerResponse ReaderResponse) handlePeerProperties() interface{} {
	readerResponse.response.CorrelationId = ReadIntFromReader(readerResponse.SocketReader)
	readerResponse.response.ResponseCode = ReadShortFromReader(readerResponse.SocketReader)

	serverPropertiesCount := ReadIntFromReader(readerResponse.SocketReader)
	serverProperties := make(map[string]string)

	for i := 0; i < int(serverPropertiesCount); i++ {
		key := ReadStringFromReader(readerResponse.SocketReader)
		value := ReadStringFromReader(readerResponse.SocketReader)
		serverProperties[key] = value
	}

	return serverProperties
}

func (readerResponse ReaderResponse) handleTune() interface{} {

	serverMaxFrameSize := ReadIntFromReader(readerResponse.SocketReader)
	serverHeartbeat := ReadIntFromReader(readerResponse.SocketReader)

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

func (readerResponse ReaderResponse) handleGenericResponse() interface{} {

	readerResponse.response.CorrelationId = ReadIntFromReader(readerResponse.SocketReader)
	readerResponse.response.ResponseCode = ReadShortFromReader(readerResponse.SocketReader)

	return readerResponse.response.ResponseCode

}
