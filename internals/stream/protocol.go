package stream

import (
	"fmt"
	"io"
)

type Response struct {
	FrameLen      int32
	CommandID     int16
	Key           int16
	Version       int16
	CorrelationId int32
	ResponseCode  int16
}

func (response *Response) ReadResponseFromStream(readerStream io.Reader) {
	response.FrameLen = ReadIntFromReader(readerStream)
	response.CommandID = ReadShortFromReader(readerStream)
	response.Version = ReadShortFromReader(readerStream)
	//response.CorrelationId = ReadIntFromReader(readerStream)
	//response.ResponseCode = ReadShortFromReader(readerStream)

	fmt.Printf("peerProperties key:%d, CorrelationId:%d, Version:%d,ResponseCode:%d \n ", response.Key, response.CorrelationId, response.Version, response.ResponseCode)

}

