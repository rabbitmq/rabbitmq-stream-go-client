package stream

import (
	"bufio"
	"bytes"
	"fmt"
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
		readerProtocol.FrameLen = ReadUInt(buffer)
		readerProtocol.CommandID = UShortExtractResponseCode(ReadUShort(buffer))
		readerProtocol.Version = ReadUShort(buffer)

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
	streamingRes.CorrelationId = ReadUInt(r)
	streamingRes.ResponseCode = UShortExtractResponseCode(ReadUShort(r))
	mechanismsCount := ReadUInt(r)
	var mechanisms []string
	for i := 0; i < int(mechanismsCount); i++ {
		mechanism := ReadString(r)
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

func (client *Client) handlePeerProperties(readProtocol *ReaderProtocol, r *bufio.Reader) {
	readProtocol.CorrelationId = ReadUInt(r)
	readProtocol.ResponseCode = UShortExtractResponseCode(ReadUShort(r))
	if readProtocol.ResponseCode != 1 {
		fmt.Printf("Errr ResponseCode: %d ", readProtocol.ResponseCode)
	}
	serverPropertiesCount := ReadUInt(r)
	serverProperties := make(map[string]string)

	for i := 0; i < int(serverPropertiesCount); i++ {
		key := ReadString(r)
		value := ReadString(r)
		serverProperties[key] = value
	}
	res, err := client.responses.GetById(readProtocol.CorrelationId)
	if err != nil {
		// TODO handle response
		return
	}
	res.code <- Code{id: readProtocol.ResponseCode}

}

func (client *Client) handleTune(r *bufio.Reader) interface{} {

	serverMaxFrameSize := ReadUInt(r)
	serverHeartbeat := ReadUInt(r)

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

func (client *Client) handleGenericResponse(readProtocol *ReaderProtocol, r *bufio.Reader) {
	readProtocol.CorrelationId = ReadUInt(r)
	readProtocol.ResponseCode = UShortExtractResponseCode(ReadUShort(r))
	res, err := client.responses.GetById(readProtocol.CorrelationId)
	if err != nil {
		// TODO handle readProtocol
		return
	}
	res.code <- Code{id: readProtocol.ResponseCode}
}

func (client *Client) handleConfirm(readProtocol *ReaderProtocol, r *bufio.Reader) interface{} {
	readProtocol.PublishID = ReadByte(r)
	//readProtocol.PublishingIdCount = ReadIntFromReader(client.reader)
	publishingIdCount := ReadUInt(r)
	//var _publishingId int64
	for publishingIdCount != 0 {
		ReadInt64(r)
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

