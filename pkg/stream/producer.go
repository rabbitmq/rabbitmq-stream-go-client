package stream

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/logs"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/message"
	"sync"
	"sync/atomic"
	"time"
)

/// ProducerResponse Section ///

// ProducerResponse is a response from the broker to the producer
// It can be a confirmation or an error
type ProducerResponse struct {
	commandId uint16
	payload   []byte
}

func (s *ProducerResponse) IsConfirm() bool {
	return s.commandId == commandPublishConfirm
}

func (s *ProducerResponse) IsPublishError() bool {
	return s.commandId == commandPublishError
}

func (s ProducerResponse) GetResponseType() uint16 {
	return s.commandId
}

// GetListOfConfirmations returns a list of confirmations for the messages sent
// It is used to check if the messages were sent correctly
// The parser is on-demand.
func (s *ProducerResponse) GetListOfConfirmations() []int64 {
	publishingIdCount := len(s.payload) / 8
	confirmationsID := make([]int64, 0, publishingIdCount)
	bufferReader := bytes.NewReader(s.payload)
	for i := 0; i < publishingIdCount; i++ {
		val := readInt64(bufferReader)
		confirmationsID = append(confirmationsID, val)
	}
	return confirmationsID
}

// PublishingIdError tuple of publishingId and errorCode
// for example an error could be: PublisherDoesNotExist
type PublishingIdError struct {
	PublishingId int64
	ErrorCode    uint16
}

// GetPublishError returns the error code and publishingId if there is an error
// during the publishing of the message
func (s *ProducerResponse) GetPublishError() []PublishingIdError {
	publishingErrorCount := len(s.payload) / (8 + // int64 for publishingId
		2) // uint16 for errorCode
	publishingErrors := make([]PublishingIdError, 0, publishingErrorCount)
	bufferReader := bytes.NewReader(s.payload)
	for i := 0; i < publishingErrorCount; i++ {
		id := readInt64(bufferReader)
		code := readUShort(bufferReader)
		publishingErrors = append(publishingErrors, PublishingIdError{id, code})
	}
	return publishingErrors
}

//// ProducerOptions Section ///

// ProducerOptions is the options for the producer
// if not specified default values will be used
// see NewProducerOptions for default values
type ProducerOptions struct {
	streamName           string
	Name                 string      // Producer name, it is useful to handle deduplication messages
	QueueSize            int         // Internal queue to handle back-pressure, low value reduces the back-pressure on the server
	BatchSize            int         // It is the batch-unCompressedSize aggregation, low value reduce the latency, high value increase the throughput
	BatchPublishingDelay int         // Period to send a batch of messages.
	SubEntrySize         int         // Size of sub Entry, to aggregate more subEntry using one publishing id
	Compression          Compression // Compression type, it is valid only if SubEntrySize > 1
}

func (po *ProducerOptions) SetProducerName(name string) *ProducerOptions {
	po.Name = name
	return po
}

func (po *ProducerOptions) SetQueueSize(size int) *ProducerOptions {
	po.QueueSize = size
	return po
}

func (po *ProducerOptions) SetBatchSize(size int) *ProducerOptions {
	po.BatchSize = size
	return po
}

func (po *ProducerOptions) SetBatchPublishingDelay(size int) *ProducerOptions {
	po.BatchPublishingDelay = size
	return po
}

func (po *ProducerOptions) SetSubEntrySize(size int) *ProducerOptions {
	po.SubEntrySize = size
	return po
}

func (po *ProducerOptions) SetCompression(compression Compression) *ProducerOptions {
	po.Compression = compression
	return po
}

func NewProducerOptions() *ProducerOptions {
	return &ProducerOptions{
		QueueSize:            defaultQueuePublisherSize,
		BatchSize:            defaultBatchSize,
		BatchPublishingDelay: defaultBatchPublishingDelay,
		SubEntrySize:         1,
		Compression:          Compression{},
	}
}

func (po *ProducerOptions) isSubEntriesBatching() bool {
	return po.SubEntrySize > 1
}

//// Producer Section ///

// pendingMessagesSequence is a struct to manage the buffered messages
// that have to be sent to the server
type pendingMessagesSequence struct {
	messages []messageSequence
	size     int // size of the buffer
}

// messageSequence contains the decoded messages, publishing id
// and the unCompressed data Size.
type messageSequence struct {
	messageBytes     []byte
	unCompressedSize int
	publishingId     int64
}

// Producer is the struct to handle the producer
// It is used to publish messages to the stream

type Producer struct {
	id uint8 // id of the producer. By protocol is a byte. In this case it is always 0 since
	// the relationship between the producer and the connection is 1:1
	options      *ProducerOptions
	sequence     int64
	mutex        *sync.Mutex
	mutexPending *sync.Mutex
	closeHandler chan Event
	status       int
	client       *Client

	/// bufferedMessagesCh channel to cache the messages to be sent
	bufferedMessagesCh chan messageSequence

	// pendingMessages contains the messages cached
	pendingMessages pendingMessagesSequence

	//producerServerResponse is the channel where the producer receives the responses from the broker
	// It is an optional field see NotifyPublishConfirmation func to enable it
	// see server_frame handlePublishError and handlePublishConfirm for details
	producerServerResponse chan ProducerResponse
}

func newProducer(client *Client, options *ProducerOptions) *Producer {
	producer := &Producer{
		id:                 0,
		options:            options,
		sequence:           0,
		mutex:              &sync.Mutex{},
		mutexPending:       &sync.Mutex{},
		closeHandler:       nil,
		status:             0,
		client:             client,
		bufferedMessagesCh: make(chan messageSequence, options.QueueSize),
		pendingMessages:    pendingMessagesSequence{},
	}
	client.coordinator.entity = producer
	return producer
}

// NotifyPublishConfirmation returns a channel where the producer receives the responses from the broker
func (producer *Producer) NotifyPublishConfirmation() chan ProducerResponse {
	var producerResponses = make(chan ProducerResponse)
	producer.producerServerResponse = producerResponses
	return producerResponses
}

func (producer *Producer) sendToChannel(data interface{}) {
	if producer.producerServerResponse != nil {
		producer.producerServerResponse <- data.(ProducerResponse)
	}
}

func (producer *Producer) lenPendingMessages() int {
	producer.mutexPending.Lock()
	defer producer.mutexPending.Unlock()
	return len(producer.pendingMessages.messages)
}

func (producer *Producer) NotifyClose() ChannelClose {
	ch := make(chan Event, 1)
	producer.closeHandler = ch
	return ch
}

func (producer *Producer) GetOptions() *ProducerOptions {
	return producer.options
}

func (producer *Producer) GetBroker() *Broker {
	return producer.client.broker
}
func (producer *Producer) setStatus(status int) {
	producer.mutex.Lock()
	defer producer.mutex.Unlock()
	producer.status = status
}

func (producer *Producer) getStatus() int {
	producer.mutex.Lock()
	defer producer.mutex.Unlock()
	return producer.status
}

func (producer *Producer) sendBufferedMessages() {

	if len(producer.pendingMessages.messages) > 0 {
		err := producer.internalBatchSend(producer.pendingMessages.messages)
		if err != nil {
			return
		}
		producer.pendingMessages.messages = producer.pendingMessages.messages[:0]
		producer.pendingMessages.size = initBufferPublishSize
	}
}

func (producer *Producer) startPublishTask() {
	go func(ch chan messageSequence) {
		var ticker = time.NewTicker(time.Duration(producer.options.BatchPublishingDelay) * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case msg, running := <-ch:
				{
					if !running {
						if producer.closeHandler != nil {
							close(producer.closeHandler)
							producer.closeHandler = nil
						}
						return
					}
					producer.mutexPending.Lock()
					if producer.pendingMessages.size+msg.unCompressedSize >= producer.client.getTuneState().
						requestedMaxFrameSize {
						producer.sendBufferedMessages()
					}
					producer.pendingMessages.size += msg.unCompressedSize
					producer.pendingMessages.messages = append(producer.pendingMessages.messages, msg)
					if len(producer.pendingMessages.messages) >= (producer.options.BatchSize) {
						producer.sendBufferedMessages()
					}
					producer.mutexPending.Unlock()
				}

			case <-ticker.C:
				producer.mutexPending.Lock()
				producer.sendBufferedMessages()
				producer.mutexPending.Unlock()
			}
		}
	}(producer.bufferedMessagesCh)

}

func (producer *Producer) Send(streamMessage message.StreamMessage) error {
	msgBytes, err := streamMessage.MarshalBinary()
	if err != nil {
		return err
	}

	if len(msgBytes)+initBufferPublishSize > producer.client.getTuneState().requestedMaxFrameSize {
		return FrameTooLarge
	}
	sequence := producer.assignPublishingID(streamMessage)
	if producer.getStatus() == open {
		producer.bufferedMessagesCh <- messageSequence{
			messageBytes:     msgBytes,
			unCompressedSize: len(msgBytes),
			publishingId:     sequence,
		}
	} else {
		return fmt.Errorf("producer id: %d  closed", producer.id)
	}

	return nil
}

func (producer *Producer) assignPublishingID(message message.StreamMessage) int64 {
	sequence := message.GetPublishingId()
	// in case of sub entry the deduplication is disabled
	if !message.HasPublishingId() || producer.options.isSubEntriesBatching() {
		sequence = atomic.AddInt64(&producer.sequence, 1)
	}
	return sequence
}

func (producer *Producer) BatchSend(batchMessages []message.StreamMessage) error {
	var messagesSequence = make([]messageSequence, len(batchMessages))
	totalBufferToSend := 0
	for i, batchMessage := range batchMessages {
		messageBytes, err := batchMessage.MarshalBinary()
		if err != nil {
			return err
		}
		sequence := producer.assignPublishingID(batchMessage)
		totalBufferToSend += len(messageBytes)
		messagesSequence[i] = messageSequence{
			messageBytes:     messageBytes,
			unCompressedSize: len(messageBytes),
			publishingId:     sequence,
		}
	}

	if totalBufferToSend+initBufferPublishSize > producer.client.tuneState.requestedMaxFrameSize {
		return FrameTooLarge
	}

	return producer.internalBatchSend(messagesSequence)
}

func (producer *Producer) GetID() uint8 {
	return producer.id
}
func (producer *Producer) internalBatchSend(messagesSequence []messageSequence) error {
	return producer.internalBatchSendProdId(messagesSequence, producer.GetID())
}

func (producer *Producer) simpleAggregation(messagesSequence []messageSequence, b *bufio.Writer) {
	for _, msg := range messagesSequence {
		r := msg.messageBytes
		writeBLong(b, msg.publishingId) // publishingId
		writeBInt(b, len(r))            // len
		b.Write(r)
	}
}

func (producer *Producer) subEntryAggregation(aggregation subEntries, b *bufio.Writer, compression Compression) {
	/// 51 messages
	// aggregation.items == (5 --> [10] messages) + (1 --> [1]message)
	for _, entry := range aggregation.items {
		writeBLong(b, entry.publishingId)
		writeBByte(b, 0x80|
			compression.value<<4) // 1=SubBatchEntryType:1,CompressionType:3,Reserved:4,
		writeBShort(b, int16(len(entry.messages)))
		writeBInt(b, entry.unCompressedSize)
		writeBInt(b, entry.sizeInBytes)
		b.Write(entry.dataInBytes)
	}
}

func (producer *Producer) aggregateEntities(msgs []messageSequence, size int, compression Compression) (subEntries, error) {
	subEntries := subEntries{}

	var entry *subEntry
	for _, msg := range msgs {
		if len(subEntries.items) == 0 || len(entry.messages) >= size {
			entry = &subEntry{}
			entry.publishingId = -1
			subEntries.items = append(subEntries.items, entry)
		}
		entry.messages = append(entry.messages, msg)
		binary := msg.messageBytes
		entry.unCompressedSize += len(binary) + 4

		// in case of subEntry we need to pick only one publishingId
		// we peek the first one of the entries
		// suppose you have 10 messages with publishingId [5..15]
		if entry.publishingId < 0 {
			entry.publishingId = msg.publishingId
		}

	}

	compressByValue(compression.value).Compress(&subEntries)

	return subEntries, nil
}

/// the producer id is always the producer.GetID(). This function is needed only for testing
// some condition, like simulate publish error, see

func (producer *Producer) internalBatchSendProdId(messagesSequence []messageSequence, producerID uint8) error {
	producer.client.socket.mutex.Lock()
	defer producer.client.socket.mutex.Unlock()
	if producer.getStatus() == closed {
		return fmt.Errorf("producer id: %d closed", producer.id)
	}

	var msgLen int
	var aggregation subEntries

	if producer.options.isSubEntriesBatching() {
		var err error
		aggregation, err = producer.aggregateEntities(messagesSequence, producer.options.SubEntrySize,
			producer.options.Compression)
		if err != nil {
			return err
		}
		msgLen += ((8 + 1 + 2 + 4 + 4) * len(aggregation.items)) + aggregation.totalSizeInBytes
	}

	if !producer.options.isSubEntriesBatching() {
		for _, msg := range messagesSequence {
			msgLen += msg.unCompressedSize + 8 + 4
		}
	}

	frameHeaderLength := initBufferPublishSize
	length := frameHeaderLength + msgLen

	writeBProtocolHeader(producer.client.socket.writer, length, commandPublish)
	writeBByte(producer.client.socket.writer, producerID)
	numberOfMessages := len(messagesSequence)
	numberOfMessages = numberOfMessages / producer.options.SubEntrySize
	if len(messagesSequence)%producer.options.SubEntrySize != 0 {
		numberOfMessages += 1
	}

	writeBInt(producer.client.socket.writer, numberOfMessages) //toExcluded - fromInclude

	if producer.options.isSubEntriesBatching() {
		producer.subEntryAggregation(aggregation, producer.client.socket.writer, producer.options.Compression)
	}

	if !producer.options.isSubEntriesBatching() {
		producer.simpleAggregation(messagesSequence, producer.client.socket.writer)
	}

	err := producer.client.socket.writer.Flush()
	if err != nil {
		producer.setStatus(closed)
		return err
	}
	return nil
}

func (producer *Producer) GetLastPublishingId() (int64, error) {
	return producer.client.queryPublisherSequence(producer.GetName(), producer.GetStreamName())
}
func (producer *Producer) Close() error {
	if producer.getStatus() == closed {
		return AlreadyClosed
	}
	producer.setStatus(closed)

	if !producer.client.socket.isOpen() {
		return fmt.Errorf("tcp connection is closed")
	}

	err := producer.client.deletePublisher(producer.id)
	if err != nil {
		logs.LogError("error delete Publisher on closing: %s", err)
	}

	err = producer.client.Close()
	if err != nil {
		logs.LogError("error during closing client: %s", err)
	}

	close(producer.bufferedMessagesCh)
	if producer.producerServerResponse != nil {
		close(producer.producerServerResponse)
	}

	return nil
}

func (producer *Producer) GetStreamName() string {
	if producer.options == nil {
		return ""
	}
	return producer.options.streamName
}

func (producer *Producer) GetName() string {
	if producer.options == nil {
		return ""
	}
	return producer.options.Name
}

func (c *Client) deletePublisher(publisherId byte) error {
	length := 2 + 2 + 4 + 1
	resp := c.coordinator.NewResponse(CommandDeletePublisher)
	correlationId := resp.correlationid
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	writeProtocolHeader(b, length, CommandDeletePublisher,
		correlationId)

	writeByte(b, publisherId)
	errWrite := c.handleWrite(b.Bytes(), resp)
	return errWrite.Err
}
