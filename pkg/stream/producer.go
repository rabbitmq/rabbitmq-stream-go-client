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

type ConfirmationStatus struct {
	inserted     time.Time
	message      message.StreamMessage
	producerID   uint8
	publishingId int64
	confirmed    bool
	err          error
	errorCode    uint16
	linkedTo     []*ConfirmationStatus
}

func (cs *ConfirmationStatus) IsConfirmed() bool {
	return cs.confirmed
}

func (cs *ConfirmationStatus) GetProducerID() uint8 {
	return cs.producerID
}

func (cs *ConfirmationStatus) GetPublishingId() int64 {
	return cs.publishingId
}

func (cs *ConfirmationStatus) GetError() error {
	return cs.err
}

func (cs *ConfirmationStatus) LinkedMessages() []*ConfirmationStatus {
	return cs.linkedTo
}

func (cs *ConfirmationStatus) GetMessage() message.StreamMessage {
	return cs.message
}

func (cs *ConfirmationStatus) GetErrorCode() uint16 {
	return cs.errorCode
}

type messageSequence struct {
	messageBytes     []byte
	unCompressedSize int
	publishingId     int64
	filterValue      string
	refMessage       *message.StreamMessage
}

type Producer struct {
	id                  uint8
	options             *ProducerOptions
	onClose             onInternalClose
	mutexUnconfirmed    *sync.Mutex
	unConfirmedMessages map[int64]*ConfirmationStatus
	sequence            int64
	mutex               *sync.RWMutex
	publishConfirm      chan []*ConfirmationStatus
	closeHandler        chan Event
	status              int

	dynamicSendCh chan *messageSequence
}

type FilterValue func(message message.StreamMessage) string

type ProducerFilter struct {
	FilterValue FilterValue
}

func NewProducerFilter(filterValue FilterValue) *ProducerFilter {
	return &ProducerFilter{
		FilterValue: filterValue,
	}
}

type ProducerOptions struct {
	client               *Client
	streamName           string
	Name                 string          // Producer name, valid for deduplication
	QueueSize            int             // Internal queue to handle back-pressure, low value reduces the back-pressure on the server
	BatchSize            int             // It is the batch-unCompressedSize aggregation, low value reduce the latency, high value increase the throughput. Valid only for the method Send()
	BatchPublishingDelay int             // Timout within the aggregation sent a batch of messages. Valid only for the method Send()
	SubEntrySize         int             // Size of sub Entry, to aggregate more subEntry using one publishing id
	Compression          Compression     // Compression type, it is valid only if SubEntrySize > 1
	ConfirmationTimeOut  time.Duration   // Time to wait for the confirmation
	ClientProvidedName   string          // Client provider name that will be shown in the management UI
	Filter               *ProducerFilter // Enable the filter feature, by default is disabled. Pointer nil
}

func (po *ProducerOptions) SetProducerName(name string) *ProducerOptions {
	po.Name = name
	return po
}

func (po *ProducerOptions) SetQueueSize(size int) *ProducerOptions {
	po.QueueSize = size
	return po
}

// SetBatchSize sets the batch size for the producer
// The batch size is the number of messages that are aggregated before sending them to the server
// The SendBatch splits the messages in multiple frames if the messages are bigger than the BatchSize
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

func (po *ProducerOptions) SetConfirmationTimeOut(duration time.Duration) *ProducerOptions {
	po.ConfirmationTimeOut = duration
	return po
}

func (po *ProducerOptions) SetClientProvidedName(name string) *ProducerOptions {
	po.ClientProvidedName = name
	return po
}

func (po *ProducerOptions) SetFilter(filter *ProducerFilter) *ProducerOptions {
	po.Filter = filter
	return po
}

func (po *ProducerOptions) IsFilterEnabled() bool {
	return po.Filter != nil
}

func NewProducerOptions() *ProducerOptions {
	return &ProducerOptions{
		QueueSize:            defaultQueuePublisherSize,
		BatchSize:            defaultBatchSize,
		BatchPublishingDelay: defaultBatchPublishingDelay,
		SubEntrySize:         1,
		Compression:          Compression{},
		ConfirmationTimeOut:  defaultConfirmationTimeOut,
		ClientProvidedName:   "go-stream-producer",
		Filter:               nil,
	}
}

func (producer *Producer) GetUnConfirmed() map[int64]*ConfirmationStatus {
	producer.mutexUnconfirmed.Lock()
	defer producer.mutexUnconfirmed.Unlock()
	return producer.unConfirmedMessages
}

func (producer *Producer) addUnConfirmedSequences(message []*messageSequence, producerID uint8) {
	producer.mutexUnconfirmed.Lock()
	defer producer.mutexUnconfirmed.Unlock()

	for _, msg := range message {
		producer.unConfirmedMessages[msg.publishingId] =
			&ConfirmationStatus{
				inserted:     time.Now(),
				message:      *msg.refMessage,
				producerID:   producerID,
				publishingId: msg.publishingId,
				confirmed:    false,
			}
	}

}
func (producer *Producer) addUnConfirmed(sequence int64, message message.StreamMessage, producerID uint8) {
	producer.mutexUnconfirmed.Lock()
	defer producer.mutexUnconfirmed.Unlock()
	producer.unConfirmedMessages[sequence] = &ConfirmationStatus{
		inserted:     time.Now(),
		message:      message,
		producerID:   producerID,
		publishingId: sequence,
		confirmed:    false,
	}
}

func (po *ProducerOptions) isSubEntriesBatching() bool {
	return po.SubEntrySize > 1
}

func (producer *Producer) removeFromConfirmationStatus(status []*ConfirmationStatus) {
	producer.mutexUnconfirmed.Lock()
	defer producer.mutexUnconfirmed.Unlock()

	for _, msg := range status {
		delete(producer.unConfirmedMessages, msg.publishingId)
		for _, linked := range msg.linkedTo {
			delete(producer.unConfirmedMessages, linked.publishingId)
		}
	}
}

func (producer *Producer) removeUnConfirmed(sequence int64) {
	producer.mutexUnconfirmed.Lock()
	defer producer.mutexUnconfirmed.Unlock()
	delete(producer.unConfirmedMessages, sequence)
}

func (producer *Producer) lenUnConfirmed() int {
	producer.mutexUnconfirmed.Lock()
	defer producer.mutexUnconfirmed.Unlock()
	return len(producer.unConfirmedMessages)
}

func (producer *Producer) getUnConfirmed(sequence int64) *ConfirmationStatus {
	producer.mutexUnconfirmed.Lock()
	defer producer.mutexUnconfirmed.Unlock()
	return producer.unConfirmedMessages[sequence]
}

func (producer *Producer) NotifyPublishConfirmation() ChannelPublishConfirm {
	ch := make(chan []*ConfirmationStatus, 1)
	producer.publishConfirm = ch
	return ch
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
	return producer.options.client.broker
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

func (producer *Producer) startUnconfirmedMessagesTimeOutTask() {

	go func() {
		for producer.getStatus() == open {
			time.Sleep(2 * time.Second)
			toRemove := make([]*ConfirmationStatus, 0)
			// check the unconfirmed messages and remove the one that are expired
			producer.mutexUnconfirmed.Lock()
			for _, msg := range producer.unConfirmedMessages {
				if time.Since(msg.inserted) > producer.options.ConfirmationTimeOut {
					msg.err = ConfirmationTimoutError
					msg.errorCode = timeoutError
					msg.confirmed = false
					toRemove = append(toRemove, msg)
				}
			}
			producer.mutexUnconfirmed.Unlock()

			if len(toRemove) > 0 {
				producer.removeFromConfirmationStatus(toRemove)
				if producer.publishConfirm != nil {
					producer.publishConfirm <- toRemove
				}
			}
		}
		time.Sleep(1 * time.Second)
		producer.flushUnConfirmedMessages(timeoutError, ConfirmationTimoutError)
	}()

}
func (producer *Producer) processSendingMessages() {

	// the queueMessages is the buffer to accumulate the messages

	// queueMessages is shared between the two goroutines
	queueMessages := make([]*messageSequence, 0)
	totalBufferToSend := 0
	mutexQueue := sync.RWMutex{}
	chSignal := make(chan struct{}, producer.options.BatchSize)
	maxFrame := producer.options.client.getTuneState().requestedMaxFrameSize

	// temporary variables to calculate the average
	sent := 0
	frames := 0
	iterations := 0

	// the waitGroup is used to wait for the two goroutines
	// the first goroutine is the one that sends the messages
	// the second goroutine is the one that accumulates the messages
	// the waitGroup is used to wait for the two goroutines
	waitGroup := sync.WaitGroup{}
	waitGroup.Add(1)
	/// send the messages in a batch
	go func() {
		waitGroup.Done()
		for range chSignal {
			lenB := 0
			mutexQueue.RLock()
			lenB = len(queueMessages)
			mutexQueue.RUnlock()

			if lenB > 0 {
				mutexQueue.Lock()
				err := producer.internalBatchSend(queueMessages)
				if err != nil {
					logs.LogError("Producer %d, error during batch send: %s", producer.GetID(), err)
				}
				sent += len(queueMessages)
				//frames += result.TotalFrames
				//if result.TotalSent != len(queueMessages) {
				//	logs.LogError("Producer %d, error during batch send: %s", producer.GetID(), err)
				//}
				queueMessages = queueMessages[:0]
				iterations++
				if iterations > 0 && iterations%1000000 == 0 {
					logs.LogInfo("Producer %d, average messages: %d, frames %d, sent:%d",
						producer.GetID(), sent/iterations, frames/iterations, sent)
					//}
				}
				totalBufferToSend = initBufferPublishSize
				mutexQueue.Unlock()
			}
		}
	}()

	waitGroup.Wait()

	waitGroup.Add(1)
	/// accumulate the messages in a buffer
	go func() {
		waitGroup.Done()
		for msg := range producer.dynamicSendCh {
			toSend := false
			mutexQueue.Lock()
			totalBufferToSend += msg.unCompressedSize
			if totalBufferToSend > maxFrame || len(queueMessages) > producer.options.BatchSize {
				toSend = true
			}
			mutexQueue.Unlock()
			if toSend {
				chSignal <- struct{}{}
			}

			mutexQueue.Lock()
			queueMessages = append(queueMessages, msg)
			mutexQueue.Unlock()
			chSignal <- struct{}{}
		}

		// the channel is closed, so we can close the signal channel
		close(chSignal)
	}()

	waitGroup.Wait()
	// The two goroutines are ready
}

// Send sends a message to the stream and returns an error if the message could not be sent.
// Send is asynchronous. The aggregation of the messages is based on the BatchSize and BatchPublishingDelay
// options. The message is sent when the aggregation is reached or the BatchPublishingDelay is reached.
func (producer *Producer) Send(streamMessage message.StreamMessage) error {
	marshalBinary, err := streamMessage.MarshalBinary()
	if err != nil {
		return err
	}
	seq := producer.assignPublishingID(streamMessage)
	filterValue := ""
	if producer.options.IsFilterEnabled() {
		filterValue = producer.options.Filter.FilterValue(streamMessage)
	}

	if len(marshalBinary) > producer.options.client.getTuneState().requestedMaxFrameSize {
		if producer.publishConfirm != nil {
			producer.publishConfirm <- []*ConfirmationStatus{
				{
					inserted:     time.Now(),
					message:      streamMessage,
					producerID:   producer.GetID(),
					publishingId: seq,
					confirmed:    false,
					err:          FrameTooLarge,
					errorCode:    responseCodeFrameTooLarge,
				},
			}
		}
		return FrameTooLarge
	}
	producer.addUnConfirmed(seq, streamMessage, producer.GetID())
	producer.dynamicSendCh <- &messageSequence{
		messageBytes:     marshalBinary,
		unCompressedSize: len(marshalBinary),
		publishingId:     seq,
		filterValue:      filterValue,
		refMessage:       &streamMessage,
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

// BatchSend sends a batch of messages to the stream and returns an error if the messages could not be sent.
// The method is synchronous. The aggregation is up to the user. The user has to aggregate the messages
// and send them in a batch.
// BatchSend is not affected by the BatchSize and BatchPublishingDelay options.
// BatchSend is the primitive method to send messages to the stream, the method Send prepares the messages and
// calls BatchSend internally.
// It automatically splits the messages in multiple frames if the total size of the messages is greater than the
// requestedMaxFrameSize.
func (producer *Producer) BatchSend(batchMessages []message.StreamMessage) error {
	maxFrame := producer.options.client.getTuneState().requestedMaxFrameSize
	var messagesSequence = make([]*messageSequence, 0)
	totalBufferToSend := 0
	for _, batchMessage := range batchMessages {
		messageBytes, err := batchMessage.MarshalBinary()
		if err != nil {
			return err
		}
		filterValue := ""
		if producer.options.IsFilterEnabled() {
			filterValue = producer.options.Filter.FilterValue(batchMessage)
		}

		totalBufferToSend += len(messageBytes)
		// if the totalBufferToSend is greater than the requestedMaxFrameSize
		// the producer sends the messages and reset the buffer
		// it splits the messages in multiple frames

		messagesSequence = append(messagesSequence, &messageSequence{
			messageBytes:     messageBytes,
			unCompressedSize: len(messageBytes),
			publishingId:     producer.assignPublishingID(batchMessage),
			filterValue:      filterValue,
			refMessage:       &batchMessage,
		})
	}
	//

	if totalBufferToSend+initBufferPublishSize > maxFrame {
		for _, msg := range messagesSequence {
			if producer.publishConfirm != nil {
				unConfirmedMessage := &ConfirmationStatus{
					inserted:     time.Now(),
					message:      *msg.refMessage,
					producerID:   producer.GetID(),
					publishingId: msg.publishingId,
					confirmed:    false,
					err:          FrameTooLarge,
					errorCode:    responseCodeFrameTooLarge,
				}
				producer.publishConfirm <- []*ConfirmationStatus{unConfirmedMessage}
			}
		}

		return FrameTooLarge
	}
	producer.addUnConfirmedSequences(messagesSequence, producer.GetID())
	return producer.internalBatchSend(messagesSequence)
}

func (producer *Producer) GetID() uint8 {
	return producer.id
}
func (producer *Producer) internalBatchSend(messagesSequence []*messageSequence) error {
	return producer.internalBatchSendProdId(messagesSequence, producer.GetID())
}

func (producer *Producer) simpleAggregation(messagesSequence []*messageSequence, b *bufio.Writer) {
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

func (producer *Producer) aggregateEntities(msgs []*messageSequence, size int, compression Compression) (subEntries, error) {
	subEntries := subEntries{}

	var entry *subEntry
	for _, msg := range msgs {
		if len(subEntries.items) == 0 || len(entry.messages) >= size {
			entry = &subEntry{
				messages: make([]*messageSequence, 0),
			}
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

		/// since there is only one publishingId
		// the others publishingId(s) are linked
		// so the client confirms all the messages
		//when the client receives the confirmation form the server
		// see: server_frame:handleConfirm/2
		// suppose you have 10 messages with publishingId [5..15]
		// the message 5 is linked to 6,7,8,9..15

		if entry.publishingId != msg.publishingId {
			unConfirmed := producer.getUnConfirmed(entry.publishingId)
			if unConfirmed != nil {
				unConfirmed.linkedTo =
					append(unConfirmed.linkedTo,
						producer.getUnConfirmed(msg.publishingId))
			}
		}
	}

	compressByValue(compression.value).Compress(&subEntries)

	return subEntries, nil
}

/// the producer id is always the producer.GetID(). This function is needed only for testing
// some condition, like simulate publish error, see

func (producer *Producer) internalBatchSendProdId(messagesSequence []*messageSequence, producerID uint8) error {
	producer.options.client.socket.mutex.Lock()
	defer producer.options.client.socket.mutex.Unlock()
	if producer.getStatus() == closed {
		return fmt.Errorf("producer id: %d closed", producer.id)
	}

	if producer.options.IsFilterEnabled() &&
		// this check is just for safety. The producer can't be created with Filter and SubEntry > 1
		!producer.options.isSubEntriesBatching() {
		return producer.sendWithFilter(messagesSequence, producerID)
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

	writeBProtocolHeader(producer.options.client.socket.writer, length, commandPublish)
	writeBByte(producer.options.client.socket.writer, producerID)
	numberOfMessages := len(messagesSequence)
	numberOfMessages = numberOfMessages / producer.options.SubEntrySize
	if len(messagesSequence)%producer.options.SubEntrySize != 0 {
		numberOfMessages += 1
	}

	writeBInt(producer.options.client.socket.writer, numberOfMessages) //toExcluded - fromInclude

	if producer.options.isSubEntriesBatching() {
		producer.subEntryAggregation(aggregation, producer.options.client.socket.writer, producer.options.Compression)
	}

	if !producer.options.isSubEntriesBatching() {
		producer.simpleAggregation(messagesSequence, producer.options.client.socket.writer)
	}

	err := producer.options.client.socket.writer.Flush() //writeAndFlush(b.Bytes())
	if err != nil {
		logs.LogError("Producer BatchSend error during flush: %s", err)
		producer.setStatus(closed)
		return err
	}
	return nil
}

func (producer *Producer) flushUnConfirmedMessages(errorCode uint16, err error) {
	producer.mutexUnconfirmed.Lock()

	for _, msg := range producer.unConfirmedMessages {
		msg.confirmed = false
		msg.err = err
		msg.errorCode = errorCode
		if producer.publishConfirm != nil {
			producer.publishConfirm <- []*ConfirmationStatus{msg}
		}
	}
	producer.unConfirmedMessages = make(map[int64]*ConfirmationStatus)

	producer.mutexUnconfirmed.Unlock()
}

func (producer *Producer) GetLastPublishingId() (int64, error) {
	return producer.options.client.queryPublisherSequence(producer.GetName(), producer.GetStreamName())
}
func (producer *Producer) Close() error {
	if producer.getStatus() == closed {
		return AlreadyClosed
	}

	producer.waitForInflightMessages()
	producer.setStatus(closed)

	if !producer.options.client.socket.isOpen() {
		return fmt.Errorf("tcp connection is closed")
	}

	err := producer.options.client.deletePublisher(producer.id)
	if err != nil {
		logs.LogError("error delete Publisher on closing: %s", err)
	}
	if producer.options.client.coordinator.ProducersCount() == 0 {
		err := producer.options.client.Close()
		if err != nil {
			logs.LogError("error during closing client: %s", err)
		}
	}

	if producer.onClose != nil {
		ch := make(chan uint8, 1)
		ch <- producer.id
		producer.onClose(ch)
		close(ch)
	}

	close(producer.dynamicSendCh)
	return nil
}

func (producer *Producer) waitForInflightMessages() {
	// during the close there cloud be pending messages
	// it waits for producer.options.BatchPublishingDelay
	// to flush the last messages
	// see issues/103

	channelLength := len(producer.dynamicSendCh)
	tentatives := 0

	for (channelLength > 0 || producer.lenUnConfirmed() > 0) && tentatives < 3 {
		logs.LogDebug("waitForInflightMessages, channel: %d - unconfirmed len: %d - retry: %d",
			channelLength,
			producer.lenUnConfirmed(), tentatives)
		time.Sleep(time.Duration(2*producer.options.BatchPublishingDelay) * time.Millisecond)
		tentatives++
	}
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

func (producer *Producer) sendWithFilter(messagesSequence []*messageSequence, producerID uint8) error {
	frameHeaderLength := initBufferPublishSize
	var msgLen int
	for _, msg := range messagesSequence {
		msgLen += msg.unCompressedSize + 8 + 4
		if msg.filterValue != "" {
			msgLen += 2 + len(msg.filterValue)
		}
	}
	length := frameHeaderLength + msgLen

	writeBProtocolHeaderVersion(producer.options.client.socket.writer, length, commandPublish, version2)
	writeBByte(producer.options.client.socket.writer, producerID)
	numberOfMessages := len(messagesSequence)
	writeBInt(producer.options.client.socket.writer, numberOfMessages)

	for _, msg := range messagesSequence {
		writeBLong(producer.options.client.socket.writer, msg.publishingId)
		if msg.filterValue != "" {
			writeBString(producer.options.client.socket.writer, msg.filterValue)
		} else {
			writeBInt(producer.options.client.socket.writer, -1)
		}
		writeBInt(producer.options.client.socket.writer, len(msg.messageBytes)) // len
		_, err := producer.options.client.socket.writer.Write(msg.messageBytes)
		if err != nil {
			return err
		}
	}

	return producer.options.client.socket.writer.Flush()

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

	err := c.coordinator.RemoveProducerById(publisherId, Event{
		Command: CommandDeletePublisher,
		Reason:  "deletePublisher",
		Err:     nil,
	})
	if err != nil {
		logs.LogWarn("producer id: %d already removed", publisherId)
	}

	return errWrite.Err
}
