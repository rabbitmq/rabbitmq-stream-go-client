package stream

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
)

type ReaderProtocol struct {
	FrameLen          uint32
	CommandID         uint16
	Key               uint16
	Version           uint16
	CorrelationId     uint32
	ResponseCode      uint16
	PublishID         uint8
	PublishingIdCount uint64
}

func (client *Client) handleResponse(conn net.Conn) {
	buffer := bufio.NewReader(conn)
	for {
		readerProtocol := &ReaderProtocol{}
		readerProtocol.FrameLen = ReadUIntFromReader(buffer)
		readerProtocol.CommandID = UShortExtractResponseCode(ReadUShortFromReader(buffer))
		readerProtocol.Version = ReadUShortFromReader(buffer)

		switch readerProtocol.CommandID {

		case CommandPeerProperties:
			{
				client.handlePeerProperties(readerProtocol, buffer)
			}
		case CommandSaslHandshake:
			{
				client.handleSaslHandshakeResponse(readerProtocol, buffer)
			}
		case CommandTune:
			{
				client.handleTune(buffer)
			}
		case CommandOpen, CommandDeclarePublisher,
			CommandDeletePublisher, CommandDeleteStream,
			CommandCreateStream, CommandSaslAuthenticate:
			{
				client.handleGenericResponse(readerProtocol, buffer)
			}

		case CommandPublishConfirm:
			{
				client.handleConfirm(readerProtocol, buffer)
			}
		case CommandHeartbeat:
			{
				client.sendHeartbeat()
			}
		default:
			{
				fmt.Printf("dont CommandID %d buff:%d \n", readerProtocol.CommandID, buffer.Buffered())
			}

		}

	}

}

func (client *Client) handleSaslHandshakeResponse(streamingRes *ReaderProtocol, r *bufio.Reader) interface{} {
	streamingRes.CorrelationId = ReadUIntFromReader(r)
	streamingRes.ResponseCode = UShortExtractResponseCode(ReadUShortFromReader(r))
	mechanismsCount := ReadUIntFromReader(r)
	var mechanisms []string
	for i := 0; i < int(mechanismsCount); i++ {
		mechanism := ReadStringFromReader(r)
		mechanisms = append(mechanisms, mechanism)
	}

	res, err := client.responses.GetById(streamingRes.CorrelationId)
	if err != nil {
		// TODO handle response
		return err
	}
	res.dataString <- mechanisms

	return mechanisms
}

func (client *Client) handlePeerProperties(streamingRes *ReaderProtocol, r *bufio.Reader) interface{} {
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
	res, err := client.responses.GetById(streamingRes.CorrelationId)
	if err != nil {
		// TODO handle response
		return err
	}
	res.isDone <- true
	return serverProperties
}

func (client *Client) handleTune(r *bufio.Reader) interface{} {

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
	res, err := client.responses.GetByName("tune")
	if err != nil {
		// TODO handle response
		return err
	}
	res.dataBytes <- b.Bytes()
	return b.Bytes()

}

func (client *Client) handleGenericResponse(response *ReaderProtocol, r *bufio.Reader) interface{} {
	response.CorrelationId = ReadUIntFromReader(r)
	response.ResponseCode = UShortExtractResponseCode(ReadUShortFromReader(r))
	if response.ResponseCode != 1 {
		fmt.Printf("Errr ResponseCode: %d \n", response.ResponseCode)

	}
	res, err := client.responses.GetById(response.CorrelationId)
	if err != nil {
		// TODO handle response
		return err
	}
	res.isDone <- true
	return response.ResponseCode
}

func (client *Client) handleConfirm(response *ReaderProtocol, r *bufio.Reader) interface{} {
	response.PublishID = ReadByteFromReader(r)
	//response.PublishingIdCount = ReadIntFromReader(client.reader)
	publishingIdCount := ReadUIntFromReader(r)
	//var _publishingId int64
	for publishingIdCount != 0 {
		ReadInt64FromReader(r)
		publishingIdCount--
	}

	return 0

}

func (client *Client) sendHeartbeat() {
	//length := 4 + 2 + 2
	//var b = bytes.NewBuffer(make([]byte, 0, length+4))
	//WriteInt(b, length)
	//WriteUShort(b, UShortEncodeResponseCode(CommandHeartbeat))
	//
	//ByteBuf bb = allocate(ctx.alloc(), 4 + 2 + 2);
	//bb.writeInt(4).writeShort(encodeRequestCode(COMMAND_HEARTBEAT)).writeShort(VERSION_1);
	//ctx.writeAndFlush(bb);

}

func ReadUShortFromReader(readerStream io.Reader) uint16 {
	var res uint16
	_ = binary.Read(readerStream, binary.BigEndian, &res)
	return res
}

func ReadUIntFromReader(readerStream io.Reader) uint32 {
	var res uint32
	_ = binary.Read(readerStream, binary.BigEndian, &res)
	return res
}

func ReadInt64FromReader(readerStream io.Reader) int64 {
	var res int64
	_ = binary.Read(readerStream, binary.BigEndian, &res)
	return res
}

func ReadByteFromReader(readerStream io.Reader) uint8 {
	var res uint8
	_ = binary.Read(readerStream, binary.BigEndian, &res)
	return res

}

func ReadStringFromReader(readerStream io.Reader) string {
	lenString := ReadUShortFromReader(readerStream)
	buff := make([]byte, lenString)
	_ = binary.Read(readerStream, binary.BigEndian, &buff)
	return string(buff)
}
