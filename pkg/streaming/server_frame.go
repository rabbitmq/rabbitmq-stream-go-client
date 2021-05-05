package streaming

import (
	"bufio"
	"bytes"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
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
		frameLen, err := readUInt(buffer)
		if err != nil {
			DEBUG("socket: %s", err)
			_ = c.Close()
			break
		}
		readerProtocol.FrameLen = frameLen
		readerProtocol.CommandID = uShortExtractResponseCode(readUShort(buffer))
		readerProtocol.Version = readUShort(buffer)

		switch readerProtocol.CommandID {

		case commandPeerProperties:
			{
				c.handlePeerProperties(readerProtocol, buffer)
			}
		case commandSaslHandshake:
			{
				c.handleSaslHandshakeResponse(readerProtocol, buffer)
			}
		case commandTune:
			{
				c.handleTune(buffer)
			}
		case commandOpen, commandDeclarePublisher,
			commandDeletePublisher, commandDeleteStream,
			commandCreateStream, commandSaslAuthenticate, commandSubscribe,
			commandUnsubscribe:
			{
				c.handleGenericResponse(readerProtocol, buffer)
			}
		case commandPublishError:
			{
				c.handlePublishError(buffer)

			}
		case commandPublishConfirm:
			{
				c.handleConfirm(readerProtocol, buffer)
			}
		case commandDeliver:
			{
				c.handleDeliver(buffer)

			}
		case commandMetadataUpdate:
			{

				c.metadataUpdateFrameHandler(buffer)
			}
		case commandCredit:
			{
				c.creditNotificationFrameHandler(readerProtocol, buffer)
			}
		case commandHeartbeat:
			{
				//DEBUG("RECEIVED Heartbeat %d buff:%d \n", readerProtocol.CommandID, buffer.Buffered())

			}
		case commandQueryOffset:
			{
				c.queryOffsetFrameHandler(readerProtocol, buffer)

			}
		case commandMetadata:
			{
				c.metadataFrameHandler(readerProtocol, buffer)
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
	streamingRes.CorrelationId, _ = readUInt(r)
	streamingRes.ResponseCode = uShortExtractResponseCode(readUShort(r))
	mechanismsCount, _ := readUInt(r)
	var mechanisms []string
	for i := 0; i < int(mechanismsCount); i++ {
		mechanism := readString(r)
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
	readProtocol.CorrelationId, _ = readUInt(r)
	readProtocol.ResponseCode = uShortExtractResponseCode(readUShort(r))
	serverPropertiesCount, _ := readUInt(r)
	serverProperties := make(map[string]string)

	for i := 0; i < int(serverPropertiesCount); i++ {
		key := readString(r)
		value := readString(r)
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

	serverMaxFrameSize, _ := readUInt(r)
	serverHeartbeat, _ := readUInt(r)

	maxFrameSize := serverMaxFrameSize
	heartbeat := serverHeartbeat

	length := 2 + 2 + 4 + 4
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	writeInt(b, length)
	writeUShort(b, uShortEncodeResponseCode(commandTune))
	writeShort(b, version1)
	writeUInt(b, maxFrameSize)
	writeUInt(b, heartbeat)
	res, err := c.coordinator.GetResponseByName("tune")
	if err != nil {
		// TODO handle response
		return err
	}
	res.data <- b.Bytes()
	return b.Bytes()

}

func (c *Client) handleGenericResponse(readProtocol *ReaderProtocol, r *bufio.Reader) {
	readProtocol.CorrelationId, _ = readUInt(r)
	readProtocol.ResponseCode = uShortExtractResponseCode(readUShort(r))
	res, err := c.coordinator.GetResponseById(readProtocol.CorrelationId)
	if err != nil {
		// TODO handle readProtocol
		return
	}
	res.code <- Code{id: readProtocol.ResponseCode}
}

func (c *Client) handleConfirm(readProtocol *ReaderProtocol, r *bufio.Reader) interface{} {

	readProtocol.PublishID = readByte(r)
	//readProtocol.PublishingIdCount = ReadIntFromReader(testEnviroment.reader)
	publishingIdCount, _ := readUInt(r)
	//var _publishingId int64
	var ids []int64
	for publishingIdCount != 0 {
		ids = append(ids, readInt64(r))
		publishingIdCount--
	}

	producer, err := c.coordinator.GetProducerById(readProtocol.PublishID)
	if err != nil {
		WARN("%s", err)
		return nil
	}
	if producer.publishConfirm != nil {
		ch := make(chan []int64, 1)
		ch <- ids
		producer.publishConfirm(ch)
		close(ch)
	}
	return 0

}

func (c *Client) handleDeliver(r *bufio.Reader) {

	subscriptionId := readByte(r)
	consumer, _ := c.coordinator.GetConsumerById(subscriptionId)

	_ = readByte(r)
	chunkType := readByte(r)
	if chunkType != 0 {
		WARN("Invalid chunkType: %d ", chunkType)
	}

	_ = readUShort(r)
	numRecords, _ := readUInt(r)
	_ = readInt64(r) // timestamp
	_ = readInt64(r) // epoch, unsigned long
	offset := readInt64(r)
	_, _ = readUInt(r)
	_, _ = readUInt(r)
	_, _ = readUInt(r)
	_, _ = readUInt(r)

	//fmt.Printf("%d - %d - %d - %d - %d - %d - %d - %d - %d - %d - %d \n", subscriptionId, b, chunkType,
	//		numEntries, numRecords, timestamp, epoch, unsigned, crc, dataLength, trailer)
	//fmt.Printf("%d numRecords %d \n", offset, numRecords)
	c.credit(subscriptionId, 1)

	var offsetLimit int64 = -1

	if consumer.parameters.offsetSpecification.isOffset() {
		offsetLimit = consumer.getOffset()
	}

	filter := offsetLimit != -1

	//messages
	var batchConsumingMessages []*amqp.Message
	for numRecords != 0 {
		entryType := peekByte(r)
		if (entryType & 0x80) == 0 {
			sizeMessage, _ := readUInt(r)

			arrayMessage := readUint8Array(r, sizeMessage)
			if filter && (offset < offsetLimit) {
				/// TODO set recordset as filtered
			} else {
				msg := &amqp.Message{}
				err := msg.UnmarshalBinary(arrayMessage)
				if err != nil {
					ERROR("error unmarshal messages: %s", err)
				}
				batchConsumingMessages = append(batchConsumingMessages, msg)
			}

		} else {
			WARN("entryType Not Handled %d", entryType)
		}
		numRecords--
		offset++
	}

	consumer.response.data <- offset
	consumer.response.messages <- batchConsumingMessages

}

func (c *Client) creditNotificationFrameHandler(readProtocol *ReaderProtocol, r *bufio.Reader) {
	readProtocol.ResponseCode = uShortExtractResponseCode(readUShort(r))
	subscriptionId := readByte(r)
	// TODO ASK WHAT TO DO HERE
	DEBUG("creditNotificationFrameHandler %d", subscriptionId)
}

func (c *Client) queryOffsetFrameHandler(readProtocol *ReaderProtocol, r *bufio.Reader) {
	c.handleGenericResponse(readProtocol, r)
	offset := readInt64(r)
	res, err := c.coordinator.GetResponseById(readProtocol.CorrelationId)
	if err != nil {
		// TODO handle readProtocol
		return
	}
	res.data <- offset
}

func (c *Client) handlePublishError(buffer *bufio.Reader) {

	publisherId := readByte(buffer)
	publishingErrorCount, _ := readUInt(buffer)
	var publishingId int64
	var code uint16
	for publishingErrorCount != 0 {
		publishingId = readInt64(buffer)
		code = readUShort(buffer)
		if c.PublishErrorListener != nil {
			c.PublishErrorListener(publisherId, publishingId, code, lookErrorCode(code))
		}
		publishingErrorCount--
	}

}

func (c *Client) metadataUpdateFrameHandler(buffer *bufio.Reader) {

	code := readUShort(buffer)
	if code == responseCodeStreamNotAvailable {
		stream := readString(buffer)
		WARN("stream %s is no longer available", stream)
		// TODO ASK WHAT TO DO HERE

		streamCh := make(chan string, 1)
		streamCh <- stream
		c.metadataListener(streamCh)
		close(streamCh)
	} else {
		//TODO handle the error, see the java code
		WARN("unsupported metadata update code %d", code)
	}
}

func (c *Client) metadataFrameHandler(readProtocol *ReaderProtocol, r *bufio.Reader) {
	readProtocol.CorrelationId, _ = readUInt(r)
	readProtocol.ResponseCode = responseCodeOk
	brokers := newBrokers()
	brokersCount, _ := readUInt(r)
	for i := 0; i < int(brokersCount); i++ {
		brokerReference := readShort(r)
		host := readString(r)
		port, _ := readUInt(r)
		brokers.Add(brokerReference, host, port)
	}

	streamsMetadata := StreamsMetadata{}.New()
	streamsCount, _ := readUInt(r)
	for i := 0; i < int(streamsCount); i++ {
		stream := readString(r)
		responseCode := readUShort(r)
		var leader *Broker
		var replicas []*Broker
		leaderReference := readShort(r)
		leader = brokers.Get(leaderReference)
		replicasCount, _ := readUInt(r)
		for i := 0; i < int(replicasCount); i++ {
			replicaReference := readShort(r)
			replicas = append(replicas, brokers.Get(replicaReference))
		}
		streamsMetadata.Add(stream, responseCode, leader, replicas)
	}

	res, err := c.coordinator.GetResponseById(readProtocol.CorrelationId)
	if err != nil {
		// TODO handle readProtocol
		return
	}
	res.code <- Code{id: readProtocol.ResponseCode}
	res.data <- streamsMetadata
}
