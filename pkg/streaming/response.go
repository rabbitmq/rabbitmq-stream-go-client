package streaming

import (
	"bufio"
	"bytes"
	"github.com/Azure/go-amqp"
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

func (c *Client) handleResponse() {
	buffer := bufio.NewReader(c.socket.connection)
	for {
		readerProtocol := &ReaderProtocol{}
		frameLen, err := ReadUInt(buffer)
		if err != nil {
			DEBUG("Socket: %s", err)
			_ = c.Close()
			break
		}
		readerProtocol.FrameLen = frameLen
		readerProtocol.CommandID = UShortExtractResponseCode(ReadUShort(buffer))
		readerProtocol.Version = ReadUShort(buffer)

		switch readerProtocol.CommandID {

		case CommandPeerProperties:
			{
				c.handlePeerProperties(readerProtocol, buffer)
			}
		case CommandSaslHandshake:
			{
				c.handleSaslHandshakeResponse(readerProtocol, buffer)
			}
		case CommandTune:
			{
				c.handleTune(buffer)
			}
		case CommandOpen, CommandDeclarePublisher,
			CommandDeletePublisher, CommandDeleteStream,
			CommandCreateStream, CommandSaslAuthenticate, CommandSubscribe,
			CommandUnsubscribe:
			{
				c.handleGenericResponse(readerProtocol, buffer)
			}
		case CommandPublishError:
			{
				c.handlePublishError(readerProtocol, buffer)

			}
		case CommandPublishConfirm:
			{
				c.handleConfirm(readerProtocol, buffer)
			}
		case CommandDeliver:
			{
				c.handleDeliver(buffer)

			}
		case CommandMetadataUpdate:
			{

				c.MetadataUpdateFrameHandler(buffer)
			}
		case CommandCredit:
			{
				c.CreditNotificationFrameHandler(readerProtocol, buffer)
			}
		case CommandHeartbeat:
			{
				//DEBUG("RECEIVED Heartbeat %d buff:%d \n", readerProtocol.CommandID, buffer.Buffered())

			}
		case CommandQueryOffset:
			{
				c.QueryOffsetFrameHandler(readerProtocol, buffer)

			}
		default:
			{
				WARN("Command not implemented %d buff:%d \n", readerProtocol.CommandID, buffer.Buffered())
				break
			}
		}
	}

}

func (c *Client) handleSaslHandshakeResponse(streamingRes *ReaderProtocol, r *bufio.Reader) interface{} {
	streamingRes.CorrelationId, _ = ReadUInt(r)
	streamingRes.ResponseCode = UShortExtractResponseCode(ReadUShort(r))
	mechanismsCount, _ := ReadUInt(r)
	var mechanisms []string
	for i := 0; i < int(mechanismsCount); i++ {
		mechanism := ReadString(r)
		mechanisms = append(mechanisms, mechanism)
	}

	res, err := c.coordinator.GetResponseById(streamingRes.CorrelationId)
	if err != nil {
		// TODO handle response
		return err
	}
	res.data <- mechanisms

	return mechanisms
}

func (c *Client) handlePeerProperties(readProtocol *ReaderProtocol, r *bufio.Reader) {
	readProtocol.CorrelationId, _ = ReadUInt(r)
	readProtocol.ResponseCode = UShortExtractResponseCode(ReadUShort(r))
	serverPropertiesCount, _ := ReadUInt(r)
	serverProperties := make(map[string]string)

	for i := 0; i < int(serverPropertiesCount); i++ {
		key := ReadString(r)
		value := ReadString(r)
		serverProperties[key] = value
	}
	res, err := c.coordinator.GetResponseById(readProtocol.CorrelationId)
	if err != nil {
		// TODO handle response
		return
	}
	res.code <- Code{id: readProtocol.ResponseCode}

}

func (c *Client) handleTune(r *bufio.Reader) interface{} {

	serverMaxFrameSize, _ := ReadUInt(r)
	serverHeartbeat, _ := ReadUInt(r)

	maxFrameSize := serverMaxFrameSize
	heartbeat := serverHeartbeat

	length := 2 + 2 + 4 + 4
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	WriteInt(b, length)
	WriteUShort(b, UShortEncodeResponseCode(CommandTune))
	WriteShort(b, Version1)
	WriteUInt(b, maxFrameSize)
	WriteUInt(b, heartbeat)
	res, err := c.coordinator.GetResponseByName("tune")
	if err != nil {
		// TODO handle response
		return err
	}
	res.data <- b.Bytes()
	return b.Bytes()

}

func (c *Client) handleGenericResponse(readProtocol *ReaderProtocol, r *bufio.Reader) {
	readProtocol.CorrelationId, _ = ReadUInt(r)
	readProtocol.ResponseCode = UShortExtractResponseCode(ReadUShort(r))
	res, err := c.coordinator.GetResponseById(readProtocol.CorrelationId)
	if err != nil {
		// TODO handle readProtocol
		return
	}
	res.code <- Code{id: readProtocol.ResponseCode}
}

func (c *Client) handleConfirm(readProtocol *ReaderProtocol, r *bufio.Reader) interface{} {
	readProtocol.PublishID = ReadByte(r)
	//readProtocol.PublishingIdCount = ReadIntFromReader(testClient.reader)
	publishingIdCount, _ := ReadUInt(r)
	//var _publishingId int64
	for publishingIdCount != 0 {
		ReadInt64(r)
		publishingIdCount--
	}

	return 0

}

func (c *Client) handleDeliver(r *bufio.Reader) {

	subscriptionId := ReadByte(r)
	consumer, _ := c.coordinator.GetConsumerById(subscriptionId)

	_ = ReadByte(r)
	_ = ReadByte(r)
	_ = ReadUShort(r)
	numRecords, _ := ReadUInt(r)
	_ = ReadInt64(r) // timestamp
	_ = ReadInt64(r) // epoch, unsigned long
	offset := ReadInt64(r)
	_, _ = ReadUInt(r)
	_, _ = ReadUInt(r)
	_, _ = ReadUInt(r)
	_, _ = ReadUInt(r)

	//fmt.Printf("%d - %d - %d - %d - %d - %d - %d - %d - %d - %d - %d \n", subscriptionId, b, chunkType,
	//		numEntries, numRecords, timestamp, epoch, unsigned, crc, dataLength, trailer)
	//fmt.Printf("%d numRecords %d \n", offset, numRecords)
	c.credit(subscriptionId, 1)

	var offsetLimit int64 = -1

	if consumer.parameters.offsetSpecification.isOffset() {
		offsetLimit = consumer.getOffset()
	}
	//if

	filter := offsetLimit != -1

	//messages
	var batchConsumingMessages []*amqp.Message
	for numRecords != 0 {
		entryType := PeekByte(r)
		if (entryType & 0x80) == 0 {
			sizeMessage, _ := ReadUInt(r)

			arrayMessage := ReadUint8Array(r, sizeMessage)
			if filter && (offset < offsetLimit) {
				/// TODO set recordset as filtered
			} else {
				msg := &amqp.Message{}
				err := msg.UnmarshalBinary(arrayMessage)
				if err != nil {
					ERROR("Error UnmarshalBinary: %s", err)
				}
				batchConsumingMessages = append(batchConsumingMessages, msg)
			}

		} else {
			WARN("entryType Not Handled %d", entryType)
		}
		numRecords--
		offset++
		//consumer.setOffset(offset)
	}


	consumer.response.data <- offset
	consumer.response.messages <- batchConsumingMessages

}

func (c *Client) CreditNotificationFrameHandler(readProtocol *ReaderProtocol, r *bufio.Reader) {
	readProtocol.ResponseCode = UShortExtractResponseCode(ReadUShort(r))
	subscriptionId := ReadByte(r)
	// TODO ASK WHAT TO DO HERE
	DEBUG("CreditNotificationFrameHandler %d \n", subscriptionId)
}

func (c *Client) QueryOffsetFrameHandler(readProtocol *ReaderProtocol, r *bufio.Reader) {
	c.handleGenericResponse(readProtocol, r)
	offset := ReadInt64(r)
	res, err := c.coordinator.GetResponseById(readProtocol.CorrelationId)
	if err != nil {
		// TODO handle readProtocol
		return
	}
	res.data <- offset
}

func (c *Client) handlePublishError(protocol *ReaderProtocol, buffer *bufio.Reader) {

	publisherId := ReadByte(buffer)
	publishingErrorCount, _ := ReadUInt(buffer)
	//client.metricsCollector.publishError(publishingErrorCount);
	var publishingId int64
	var code uint16
	for publishingErrorCount != 0 {
		publishingId = ReadInt64(buffer)
		code = ReadUShort(buffer)
		if c.PublishErrorListener != nil {
			c.PublishErrorListener(publisherId, publishingId, code)
		}
		publishingErrorCount--
	}

}

func (c *Client) MetadataUpdateFrameHandler(buffer *bufio.Reader) {

	code := ReadUShort(buffer)
	if code == ResponseCodeStreamNotAvailable {
		stream := ReadString(buffer)
		WARN("Stream %s is no longer available", stream)
		// TODO ASK WHAT TO DO HERE
		//client.metadataListener.handle(stream, code)
	} else {
		//TODO handle the error, see the java code
		WARN("Unsupported metadata update code %d", code)
	}
}
