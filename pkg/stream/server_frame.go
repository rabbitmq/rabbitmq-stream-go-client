package stream

import (
	"bufio"
	"bytes"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/logs"
	"hash/crc32"
	"io"
	"time"
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

func logErrorCommand(error error, details string) {
	if error != nil {
		logs.LogError("Error handling command response: %s - details: %s", error, details)
	}
}

func (c *Client) handleResponse() {
	buffer := bufio.NewReader(c.socket.connection)
	for {
		readerProtocol := &ReaderProtocol{}
		frameLen, err := readUInt(buffer)
		if err != nil {
			logs.LogDebug("Read connection failed: %s", err)
			_ = c.Close()
			break
		}
		c.setLastHeartBeat(time.Now())
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
		case commandDeclarePublisher,
			CommandDeletePublisher, commandDeleteStream,
			commandCreateStream, commandSaslAuthenticate, commandSubscribe,
			CommandUnsubscribe, commandCreateSuperStream, commandDeleteSuperStream:
			{
				c.handleGenericResponse(readerProtocol, buffer)
			}
		case commandOpen:
			{
				c.commandOpen(readerProtocol, buffer)
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
		case commandQueryPublisherSequence:
			{
				c.queryPublisherSequenceFrameHandler(readerProtocol, buffer)
			}
		case CommandMetadataUpdate:
			{

				c.metadataUpdateFrameHandler(buffer)
			}
		case commandCredit:
			{
				c.creditNotificationFrameHandler(readerProtocol, buffer)
			}
		case commandHeartbeat:
			{

				c.handleHeartbeat()

			}
		case CommandQueryOffset:
			{
				c.queryOffsetFrameHandler(readerProtocol, buffer)

			}
		case commandStreamStatus:
			{
				c.streamStatusFrameHandler(readerProtocol, buffer)
			}
		case commandMetadata:
			{
				c.metadataFrameHandler(readerProtocol, buffer)
			}
		case CommandClose:
			{
				c.closeFrameHandler(readerProtocol, buffer)
			}
		case commandExchangeVersion:
			{
				c.handleExchangeVersionResponse(readerProtocol, buffer)
			}
		case consumerUpdateQueryResponse:
			{
				c.handleConsumerUpdate(readerProtocol, buffer)
			}
		case commandQueryPartition:
			{
				c.handleQueryPartitions(readerProtocol, buffer)
			}
		case commandQueryRoute:
			{
				c.handleQueryRoute(readerProtocol, buffer)
			}
		default:
			{
				logs.LogWarn("Command not implemented %d buff:%d \n", readerProtocol.CommandID, buffer.Buffered())
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
	logErrorCommand(err, "handlePeerProperties")
	res.code <- Code{id: readProtocol.ResponseCode}
	res.data <- serverProperties

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
	logErrorCommand(err, "handleTune")
	res.data <- b.Bytes()
	return b.Bytes()

}

func (c *Client) handleGenericResponse(readProtocol *ReaderProtocol, r *bufio.Reader) {
	readProtocol.CorrelationId, _ = readUInt(r)
	readProtocol.ResponseCode = uShortExtractResponseCode(readUShort(r))
	res, err := c.coordinator.GetResponseById(readProtocol.CorrelationId)
	logErrorCommand(err, "handleGenericResponse")
	res.code <- Code{id: readProtocol.ResponseCode}
}

func (c *Client) commandOpen(readProtocol *ReaderProtocol, r *bufio.Reader) {
	readProtocol.CorrelationId, _ = readUInt(r)
	readProtocol.ResponseCode = uShortExtractResponseCode(readUShort(r))
	clientProperties := ConnectionProperties{}
	connectionPropertiesCount, _ := readUInt(r)
	for i := 0; i < int(connectionPropertiesCount); i++ {
		v := readString(r)
		switch v {

		case "advertised_host":
			{
				clientProperties.host = readString(r)
			}
		case "advertised_port":
			{
				clientProperties.port = readString(r)
			}

		}

	}

	res, err := c.coordinator.GetResponseById(readProtocol.CorrelationId)
	logErrorCommand(err, "commandOpen")
	res.code <- Code{id: readProtocol.ResponseCode}
	res.data <- clientProperties

}

func (c *Client) handleConfirm(readProtocol *ReaderProtocol, r *bufio.Reader) interface{} {

	readProtocol.PublishID = readByte(r)
	//readProtocol.PublishingIdCount = ReadIntFromReader(testEnvironment.reader)
	publishingIdCount, _ := readUInt(r)
	//var _publishingId int64
	producer, err := c.coordinator.GetProducerById(readProtocol.PublishID)
	if err != nil {
		logs.LogWarn("can't find the producer during confirmation: %s", err)
		return nil
	}
	var unConfirmed []*ConfirmationStatus
	for publishingIdCount != 0 {
		seq := readInt64(r)

		m := producer.getUnConfirmed(seq)
		if m != nil {
			m.confirmed = true
			unConfirmed = append(unConfirmed, m)
			producer.removeUnConfirmed(m.publishingId)

			// in case of sub-batch entry the client receives only
			// one publishingId (or sequence)
			// so the other messages are confirmed using the linkedTo
			for _, message := range m.linkedTo {
				message.confirmed = true
				unConfirmed = append(unConfirmed, message)
				producer.removeUnConfirmed(message.publishingId)
			}
		}
		//} else {
		//logs.LogWarn("message %d not found in confirmation", seq)
		//}
		publishingIdCount--
	}

	producer.mutex.Lock()
	if producer.publishConfirm != nil {
		producer.publishConfirm <- unConfirmed
	}
	producer.mutex.Unlock()

	return 0
}

func (c *Client) queryPublisherSequenceFrameHandler(readProtocol *ReaderProtocol, r *bufio.Reader) {

	readProtocol.CorrelationId, _ = readUInt(r)
	readProtocol.ResponseCode = uShortExtractResponseCode(readUShort(r))
	sequence := readInt64(r)
	res, err := c.coordinator.GetResponseById(readProtocol.CorrelationId)
	logErrorCommand(err, "queryPublisherSequenceFrameHandler")
	res.code <- Code{id: readProtocol.ResponseCode}
	res.data <- sequence
}
func (c *Client) handleDeliver(r *bufio.Reader) {

	subscriptionId := readByte(r)
	consumer, err := c.coordinator.GetConsumerById(subscriptionId)
	if err != nil {
		logs.LogError("Handle Deliver consumer not found %s", err)
		return

	}

	_ = readByte(r)
	chunkType := readByte(r)
	if chunkType != 0 {
		logs.LogWarn("Invalid chunkType: %d ", chunkType)
	}

	numEntries := readUShort(r)
	numRecords, _ := readUInt(r)
	_ = readInt64(r)       // timestamp
	_ = readInt64(r)       // epoch, unsigned long
	offset := readInt64(r) // offset position
	crc, _ := readUInt(r)  /// crc and dataLength are needed to calculate the CRC
	dataLength, _ := readUInt(r)
	_, _ = readUInt(r)
	_, _ = readUInt(r)

	c.credit(subscriptionId, 1)

	var offsetLimit int64 = -1

	// we can have two cases
	// 1. single active consumer is enabled
	// 2. single active consumer is not enabled

	// single active consumer is enabled
	// we need to check if the consumer has an offset
	// if the consumer has an offset we need to filter the messages

	if consumer.options.IsSingleActiveConsumerEnabled() {
		if consumer.options.SingleActiveConsumer.offsetSpecification.isOffset() {
			offsetLimit = consumer.options.SingleActiveConsumer.offsetSpecification.offset
		}
	} else {
		// single active consumer is not enabled
		// So the standard offset is used
		if consumer.options.Offset.isOffset() {
			offsetLimit = consumer.GetOffset()
		}
	}

	filter := offsetLimit != -1

	//messages
	batchConsumingMessages := make(offsetMessages, 0, numRecords)
	var chunk chunkInfo
	chunk.numEntries = numEntries
	var bytesBuffer = make([]byte, int(dataLength))
	_, err = io.ReadFull(r, bytesBuffer)
	logErrorCommand(err, "handleDeliver")

	/// headers ---> payload -> messages

	if consumer.options.CRCCheck {
		checkSum := crc32.ChecksumIEEE(bytesBuffer)
		if crc != checkSum {
			logs.LogError("Error during the checkSum, expected %d, checksum %d", crc, checkSum)
			panic("Error during CRC")
		} /// ???
	}

	bufferReader := bytes.NewReader(bytesBuffer)
	dataReader := bufio.NewReader(bufferReader)

	for numRecords != 0 {
		entryType, err := peekByte(dataReader)

		if err != nil {
			if err == io.EOF {
				logs.LogDebug("EOF reading entryType %s ", err)
				return
			} else {
				logs.LogWarn("error reading entryType %s ", err)
			}
		}
		if (entryType & 0x80) == 0 {
			batchConsumingMessages = c.decodeMessage(dataReader,
				filter,
				offset,
				offsetLimit,
				batchConsumingMessages)
			numRecords--
			offset++
		} else {
			entryType, _ := readByteError(dataReader)
			// sub-batch case.
			numRecordsInBatch := readUShort(dataReader)
			uncompressedDataSize, _ := readUInt(dataReader) //uncompressedDataSize
			dataSize, _ := readUInt(dataReader)
			numRecords -= uint32(numRecordsInBatch)
			compression := (entryType & 0x70) >> 4 //compression
			uncompressedReader := compressByValue(compression).UnCompress(dataReader,
				dataSize,
				uncompressedDataSize)

			for numRecordsInBatch != 0 {
				batchConsumingMessages = c.decodeMessage(uncompressedReader,
					filter,
					offset,
					offsetLimit,
					batchConsumingMessages)
				numRecordsInBatch--
				offset++
			}

		}
	}
	chunk.offsetMessages = batchConsumingMessages
	if consumer.getStatus() == open {
		consumer.response.chunkForConsumer <- chunk
	} else {
		logs.LogDebug("The consumer %s for the stream %s is closed during the chunk dispatching. "+
			"Messages won't dispatched", consumer.GetName(), consumer.GetStreamName())
	}

}

func (c *Client) decodeMessage(r *bufio.Reader, filter bool, offset int64, offsetLimit int64, batchConsumingMessages offsetMessages) offsetMessages {
	sizeMessage, _ := readUInt(r)
	arrayMessage := readUint8Array(r, sizeMessage)
	if filter && (offset < offsetLimit) {
		/// TODO set recordset as filtered
	} else {
		msg := &amqp.Message{}
		err := msg.UnmarshalBinary(arrayMessage)
		logErrorCommand(err, "error unmarshal messages")
		batchConsumingMessages = append(batchConsumingMessages,
			&offsetMessage{offset: offset, message: msg})
	}
	return batchConsumingMessages
}

func (c *Client) creditNotificationFrameHandler(readProtocol *ReaderProtocol,
	r *bufio.Reader) {
	readProtocol.ResponseCode = uShortExtractResponseCode(readUShort(r))
	subscriptionId := readByte(r)
	consumer, err := c.coordinator.GetConsumerById(subscriptionId)
	if err != nil {
		logs.LogWarn("received a credit for an unknown subscriptionId: %d", subscriptionId)
		return
	}

	if consumer != nil && consumer.getStatus() == closed {
		logs.LogDebug("received a credit for a closed consumer %d", subscriptionId)
		return
	}

}

func (c *Client) queryOffsetFrameHandler(readProtocol *ReaderProtocol,
	r *bufio.Reader) {
	c.handleGenericResponse(readProtocol, r)
	offset := readInt64(r)
	res, err := c.coordinator.GetResponseById(readProtocol.CorrelationId)
	logErrorCommand(err, "queryOffsetFrameHandler")
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
		producer, err := c.coordinator.GetProducerById(publisherId)
		if err != nil {
			logs.LogWarn("producer id %d not found, publish error :%s", publisherId, lookErrorCode(code))
			producer = &Producer{unConfirmedMessages: map[int64]*ConfirmationStatus{}}
		} else {
			unConfirmedMessage := producer.getUnConfirmed(publishingId)

			producer.mutex.Lock()

			if producer.publishConfirm != nil && unConfirmedMessage != nil {
				unConfirmedMessage.errorCode = code
				unConfirmedMessage.err = lookErrorCode(code)
				producer.publishConfirm <- []*ConfirmationStatus{unConfirmedMessage}
			}
			producer.mutex.Unlock()
			producer.removeUnConfirmed(publishingId)
		}
		publishingErrorCount--
	}

}

func (c *Client) metadataUpdateFrameHandler(buffer *bufio.Reader) {
	code := readUShort(buffer)
	if code == responseCodeStreamNotAvailable {
		stream := readString(buffer)
		logs.LogDebug("stream %s is no longer available", stream)
		c.mutex.Lock()
		c.metadataListener <- metaDataUpdateEvent{
			StreamName: stream,
			code:       responseCodeStreamNotAvailable,
		}
		c.mutex.Unlock()

	} else {
		//TODO handle the error, see the java code
		logs.LogWarn("unsupported metadata update code %d", code)
	}
}

func (c *Client) streamStatusFrameHandler(readProtocol *ReaderProtocol,
	r *bufio.Reader) {

	c.handleGenericResponse(readProtocol, r)

	count, _ := readUInt(r)
	streamStatus := make(map[string]int64)

	for i := 0; i < int(count); i++ {
		key := readString(r)
		value := readInt64(r)
		streamStatus[key] = value
	}
	res, err := c.coordinator.GetResponseById(readProtocol.CorrelationId)
	logErrorCommand(err, "streamStatusFrameHandler")
	res.code <- Code{id: readProtocol.ResponseCode}
	res.data <- streamStatus

}

func (c *Client) metadataFrameHandler(readProtocol *ReaderProtocol,
	r *bufio.Reader) {
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
	logErrorCommand(err, "metadataFrameHandler")

	res.code <- Code{id: readProtocol.ResponseCode}
	res.data <- streamsMetadata
}

func (c *Client) closeFrameHandler(readProtocol *ReaderProtocol,
	r *bufio.Reader) {
	readProtocol.CorrelationId, _ = readUInt(r)
	readProtocol.ResponseCode = uShortExtractResponseCode(readUShort(r))
	closeReason := readString(r)
	logs.LogDebug("Received close from server, reason: %s %s %d", lookErrorCode(readProtocol.ResponseCode),
		closeReason, readProtocol.ResponseCode)

	length := 2 + 2 + 4 + 2
	var b = bytes.NewBuffer(make([]byte, 0, length))
	writeProtocolHeader(b, length, uShortEncodeResponseCode(CommandClose),
		int(readProtocol.CorrelationId))
	writeUShort(b, responseCodeOk)

	err := c.socket.writeAndFlush(b.Bytes())
	logErrorCommand(err, "Socket write buffer closeFrameHandler")

}

func (c *Client) handleConsumerUpdate(readProtocol *ReaderProtocol, r *bufio.Reader) {
	readProtocol.CorrelationId, _ = readUInt(r)
	subscriptionId := readByte(r)
	isActive := readByte(r)
	consumer, err := c.coordinator.GetConsumerById(subscriptionId)

	logErrorCommand(err, "handleConsumerUpdate")
	if consumer == nil {
		logs.LogWarn("consumer not found %d. The consumer maybe removed before the update", subscriptionId)
		return
	}
	consumer.setPromotedAsActive(isActive == 1)
	responseOff := consumer.options.SingleActiveConsumer.ConsumerUpdate(consumer.GetStreamName(),
		isActive == 1)
	consumer.options.SingleActiveConsumer.offsetSpecification = responseOff

	if isActive == 1 {
		consumer.setCurrentOffset(responseOff.offset)
	}

	err = consumer.writeConsumeUpdateOffsetToSocket(readProtocol.CorrelationId, responseOff)
	logErrorCommand(err, "handleConsumerUpdate writeConsumeUpdateOffsetToSocket")
}

func (c *Client) handleQueryPartitions(readProtocol *ReaderProtocol, r *bufio.Reader) {
	readProtocol.CorrelationId, _ = readUInt(r)
	readProtocol.ResponseCode = uShortExtractResponseCode(readUShort(r))
	partitions := make([]string, 0)
	partitionsCount, _ := readUInt(r)
	for i := 0; i < int(partitionsCount); i++ {
		partition := readString(r)
		partitions = append(partitions, partition)
	}
	res, err := c.coordinator.GetResponseById(readProtocol.CorrelationId)
	logErrorCommand(err, "handleQueryPartitions")
	res.code <- Code{id: readProtocol.ResponseCode}
	res.data <- partitions
}

func (c *Client) handleQueryRoute(readProtocol *ReaderProtocol, r *bufio.Reader) {
	readProtocol.CorrelationId, _ = readUInt(r)
	readProtocol.ResponseCode = uShortExtractResponseCode(readUShort(r))
	numStreams, _ := readUInt(r)

	routes := make([]string, 0)
	for i := 0; i < int(numStreams); i++ {
		route := readString(r)
		routes = append(routes, route)
	}

	res, err := c.coordinator.GetResponseById(readProtocol.CorrelationId)
	logErrorCommand(err, "handleQueryRoute")
	res.code <- Code{id: readProtocol.ResponseCode}
	res.data <- routes
}

func (c *Client) handleExchangeVersionResponse(readProtocol *ReaderProtocol, r *bufio.Reader) {
	readProtocol.CorrelationId, _ = readUInt(r)
	readProtocol.ResponseCode = uShortExtractResponseCode(readUShort(r))
	commandsSize, _ := readUInt(r)
	commands := make([]commandVersion, 0)
	for i := 0; i < int(commandsSize); i++ {
		commandKey := readUShort(r)
		minVersion := readUShort(r)
		maxVersion := readUShort(r)
		commands = append(commands, newCommandVersionResponse(minVersion, maxVersion, commandKey))
	}
	res, err := c.coordinator.GetResponseById(readProtocol.CorrelationId)
	logErrorCommand(err, "handleExchangeVersionResponse")
	res.code <- Code{id: readProtocol.ResponseCode}
	res.data <- commands
}

func (c *Client) handleHeartbeat() {
	logs.LogDebug("Heart beat received at %s", time.Now())
	c.setLastHeartBeat(time.Now())
}
