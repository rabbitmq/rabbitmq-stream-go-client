package stream

import (
	"bufio"
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/logs"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/message"
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

func (cs *ConfirmationStatus) updateStatus(errorCode uint16, confirmed bool) {
	cs.confirmed = confirmed
	if confirmed {
		return
	}
	cs.errorCode = errorCode
	cs.err = lookErrorCode(errorCode)
}

type messageSequence struct {
	sourceMsg    message.StreamMessage
	messageBytes []byte
	publishingId int64
	filterValue  string
}

type Producer struct {
	id          uint8
	options     *ProducerOptions
	onClose     onInternalClose
	unConfirmed *unConfirmed
	sequence    int64
	mutex       *sync.RWMutex

	closeHandler              chan Event
	status                    int
	confirmationTimeoutTicker *time.Ticker
	doneTimeoutTicker         chan struct{}

	confirmMutex        *sync.Mutex
	publishConfirmation chan []*ConfirmationStatus

	pendingSequencesQueue *BlockingQueue[*messageSequence]
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
	client     *Client
	streamName string
	// Producer name.  You need to set it to enable the deduplication feature.
	//  Deduplication is a feature that allows the producer to avoid sending duplicate messages to the stream.
	// see: https://www.rabbitmq.com/blog/2021/07/28/rabbitmq-streams-message-deduplication for more information.
	// Don't use it if you don't need the deduplication.
	Name string
	// Deprecated: starting from 1.5.0 the QueueSize is deprecated, and it will be removed in the next releases
	// It is not used anymore given the dynamic batching
	QueueSize int // Internal queue to handle back-pressure, low value reduces the back-pressure on the server
	BatchSize int // It is the batch-unCompressedSize aggregation, low value reduce the latency, high value increase the throughput. Valid only for the method Send()
	// Deprecated: starting from 1.5.0 the SetBatchPublishingDelay is deprecated, and it will be removed in the next releases
	// It is not used anymore given the dynamic batching
	BatchPublishingDelay int // Timout within the aggregation sent a batch of messages. Valid only for the method Send()
	// Size of sub Entry, to aggregate more subEntry using one publishing id
	SubEntrySize int
	// Compression type, it is valid only if SubEntrySize > 1
	// The messages can be compressed before sending them to the server
	Compression Compression
	// Time to wait for the confirmation, see the unConfirmed structure
	ConfirmationTimeOut time.Duration
	// Client provider name that will be shown in the management UI
	ClientProvidedName string
	// Enable the filter feature, by default is disabled. Pointer nil
	Filter *ProducerFilter
}

// SetProducerName sets the producer name. This name is used to enable the deduplication feature.
// See ProducerOptions.Name for more details.
// Don't use it if you don't need the deduplication.
func (po *ProducerOptions) SetProducerName(name string) *ProducerOptions {
	po.Name = name
	return po
}

// Deprecated: starting from 1.5.0 the SetQueueSize is deprecated, and it will be removed in the next releases
// It is not used anymore given the dynamic batching
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

// Deprecated: starting from 1.5.0 the SetBatchPublishingDelay is deprecated, and it will be removed in the next releases
// It is not used anymore given the dynamic batching
func (po *ProducerOptions) SetBatchPublishingDelay(size int) *ProducerOptions {
	po.BatchPublishingDelay = size
	return po
}

// SetSubEntrySize See the ProducerOptions.SubEntrySize for more details
func (po *ProducerOptions) SetSubEntrySize(size int) *ProducerOptions {
	po.SubEntrySize = size
	return po
}

// SetCompression sets the compression for the producer. See ProducerOptions.Compression for more details
func (po *ProducerOptions) SetCompression(compression Compression) *ProducerOptions {
	po.Compression = compression
	return po
}

// SetConfirmationTimeOut sets the time to wait for the confirmation. See ProducerOptions.ConfirmationTimeOut for more details
func (po *ProducerOptions) SetConfirmationTimeOut(duration time.Duration) *ProducerOptions {
	po.ConfirmationTimeOut = duration
	return po
}

// SetClientProvidedName sets the client provided name that will be shown in the management UI
func (po *ProducerOptions) SetClientProvidedName(name string) *ProducerOptions {
	po.ClientProvidedName = name
	return po
}

// SetFilter sets the filter for the producer. See ProducerOptions.Filter for more details
func (po *ProducerOptions) SetFilter(filter *ProducerFilter) *ProducerOptions {
	po.Filter = filter
	return po
}

// IsFilterEnabled returns true if the filter is enabled
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

func (po *ProducerOptions) isSubEntriesBatching() bool {
	return po.SubEntrySize > 1
}

// NotifyPublishConfirmation returns a channel that receives the confirmation status of the messages sent by the producer.
func (producer *Producer) NotifyPublishConfirmation() ChannelPublishConfirm {
	ch := make(chan []*ConfirmationStatus, 1)
	producer.publishConfirmation = ch
	return ch
}

// NotifyClose returns a channel that receives the close event of the producer.
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
		for {
			select {
			case <-producer.doneTimeoutTicker:
				logs.LogDebug("producer %d timeout thread closed", producer.id)
				return
			case <-producer.confirmationTimeoutTicker.C:
				// check the unconfirmed messages and remove the one that are expired
				if producer.getStatus() == open {
					toRemove := producer.unConfirmed.extractWithTimeOut(producer.options.ConfirmationTimeOut)
					if len(toRemove) > 0 {
						producer.sendConfirmationStatus(toRemove)
					}
				} else {
					logs.LogInfo("producer %d confirmationTimeoutTicker closed", producer.id)
					return
				}
			}
		}
	}()

}

func (producer *Producer) sendConfirmationStatus(status []*ConfirmationStatus) {
	producer.confirmMutex.Lock()
	defer producer.confirmMutex.Unlock()
	if producer.publishConfirmation != nil {
		producer.publishConfirmation <- status
	}
}

func (producer *Producer) closeConfirmationStatus() {
	producer.confirmMutex.Lock()
	defer producer.confirmMutex.Unlock()
	if producer.publishConfirmation != nil {
		close(producer.publishConfirmation)
		producer.publishConfirmation = nil
	}
}

// processPendingSequencesQueue aggregates the messages sequence in the queue and sends them to the server
// messages coming form the Send method through the pendingSequencesQueue
func (producer *Producer) processPendingSequencesQueue() {

	maxFrame := producer.options.client.getTuneState().requestedMaxFrameSize
	go func() {
		sequenceToSend := make([]*messageSequence, 0)
		totalBufferToSend := initBufferPublishSize
		for msg := range producer.pendingSequencesQueue.GetChannel() {
			var lastError error

			if producer.pendingSequencesQueue.IsStopped() {
				// add also the last message to sequenceToSend
				// otherwise it will be lost
				sequenceToSend = append(sequenceToSend, msg)
				break
			}
			// There is something in the queue. Checks the buffer is still less than the maxFrame
			totalBufferToSend += len(msg.messageBytes)
			if totalBufferToSend > maxFrame {
				// if the totalBufferToSend is greater than the requestedMaxFrameSize
				// the producer sends the messages and reset the buffer
				producer.unConfirmed.addFromSequences(sequenceToSend, producer.GetID())
				lastError = producer.internalBatchSend(sequenceToSend)
				sequenceToSend = sequenceToSend[:0]
				totalBufferToSend = initBufferPublishSize
			}

			sequenceToSend = append(sequenceToSend, msg)

			// if producer.pendingSequencesQueue.IsEmpty() means that the queue is empty so the producer is not sending
			// the messages during the checks of the buffer. In this case
			if producer.pendingSequencesQueue.IsEmpty() || len(sequenceToSend) >= producer.options.BatchSize {
				if len(sequenceToSend) > 0 {
					producer.unConfirmed.addFromSequences(sequenceToSend, producer.GetID())
					lastError = producer.internalBatchSend(sequenceToSend)
					sequenceToSend = sequenceToSend[:0]
					totalBufferToSend += initBufferPublishSize
				}
			}
			if lastError != nil {
				logs.LogError("error during sending messages: %s", lastError)
			}
		}

		// just in case there are messages in the buffer
		// not matter is sent or not the messages will be timed out
		if len(sequenceToSend) > 0 {
			producer.unConfirmed.addFromSequences(sequenceToSend, producer.GetID())
		}

	}()
	logs.LogDebug("producer %d processPendingSequencesQueue closed", producer.id)
}

func (producer *Producer) assignPublishingID(message message.StreamMessage) int64 {
	sequence := message.GetPublishingId()
	// in case of sub entry the deduplication is disabled
	if !message.HasPublishingId() || producer.options.isSubEntriesBatching() {
		sequence = atomic.AddInt64(&producer.sequence, 1)
	}
	return sequence
}

func (producer *Producer) fromMessageToMessageSequence(streamMessage message.StreamMessage) (*messageSequence, error) {
	marshalBinary, err := streamMessage.MarshalBinary()
	if err != nil {
		return nil, err
	}
	seq := producer.assignPublishingID(streamMessage)
	filterValue := ""
	if producer.options.IsFilterEnabled() {
		filterValue = producer.options.Filter.FilterValue(streamMessage)
	}
	msqSeq := &messageSequence{
		sourceMsg:    streamMessage,
		messageBytes: marshalBinary,
		publishingId: seq,
		filterValue:  filterValue,
	}
	return msqSeq, nil
}

// Send sends a message to the stream and returns an error if the message could not be sent.
// The Send is asynchronous. The message is sent to a channel ant then other goroutines aggregate and sent the messages
// The Send is dynamic so the number of messages sent decided internally based on the BatchSize
// and the messages in the buffer. The aggregation is up to the client.
// returns an error if the message could not be sent for marshal problems or if the buffer is too large
func (producer *Producer) Send(streamMessage message.StreamMessage) error {
	messageSeq, err := producer.fromMessageToMessageSequence(streamMessage)
	if err != nil {
		return err
	}
	if producer.getStatus() == closed {
		cs := &ConfirmationStatus{
			inserted:     time.Now(),
			message:      streamMessage,
			producerID:   producer.GetID(),
			publishingId: messageSeq.publishingId,
		}
		cs.updateStatus(entityClosed, false)

		producer.sendConfirmationStatus([]*ConfirmationStatus{cs})
		return fmt.Errorf("producer id: %d closed", producer.id)
	}

	if len(messageSeq.messageBytes) > defaultMaxFrameSize {
		producer.unConfirmed.addFromSequences([]*messageSequence{messageSeq}, producer.GetID())
		tooLarge := producer.unConfirmed.extractWithError(messageSeq.publishingId, responseCodeFrameTooLarge)
		producer.sendConfirmationStatus([]*ConfirmationStatus{tooLarge})
		return FrameTooLarge
	}

	// se the processPendingSequencesQueue function
	err = producer.pendingSequencesQueue.Enqueue(messageSeq)
	if err != nil {
		return fmt.Errorf("error during enqueue message: %s pending queue closed. Producer id: %d ", err, producer.id)
	}
	return nil
}

// BatchSend sends a batch of messages to the stream and returns an error if the messages could not be sent.
// The method is synchronous.The aggregation is up to the user. The user has to aggregate the messages
// and send them in a batch.
// BatchSend is not affected by the BatchSize and BatchPublishingDelay options.
// returns an error if the message could not be sent for marshal problems or if the buffer is too large
func (producer *Producer) BatchSend(batchMessages []message.StreamMessage) error {
	maxFrame := defaultMaxFrameSize
	var messagesSequences = make([]*messageSequence, 0, len(batchMessages))
	totalBufferToSend := 0

	for _, batchMessage := range batchMessages {
		messageSeq, err := producer.fromMessageToMessageSequence(batchMessage)
		if err != nil {
			return err
		}

		totalBufferToSend += len(messageSeq.messageBytes)
		messagesSequences = append(messagesSequences, messageSeq)
	}

	if len(messagesSequences) > 0 {
		producer.unConfirmed.addFromSequences(messagesSequences, producer.GetID())
	}

	if producer.getStatus() == closed {
		for _, msg := range messagesSequences {
			m := producer.unConfirmed.extractWithError(msg.publishingId, entityClosed)
			producer.sendConfirmationStatus([]*ConfirmationStatus{m})
		}
		return fmt.Errorf("producer id: %d closed", producer.id)
	}

	if totalBufferToSend+initBufferPublishSize > maxFrame {
		// if the totalBufferToSend is greater than the requestedMaxFrameSize
		// all the messages are unconfirmed

		for _, msg := range messagesSequences {
			m := producer.unConfirmed.extractWithError(msg.publishingId, responseCodeFrameTooLarge)
			producer.sendConfirmationStatus([]*ConfirmationStatus{m})
		}
		return FrameTooLarge
	}

	return producer.internalBatchSend(messagesSequences)
}

func (producer *Producer) GetID() uint8 {
	return producer.id
}

func (producer *Producer) internalBatchSend(messagesSequence []*messageSequence) error {
	return producer.internalBatchSendProdId(messagesSequence, producer.GetID())
}

func (producer *Producer) simpleAggregation(messagesSequence []*messageSequence,
	b *bufio.Writer) {
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
			producer.unConfirmed.link(entry.publishingId, msg.publishingId)
		}
	}

	compressByValue(compression.value).Compress(&subEntries)

	return subEntries, nil
}

// / the producer id is always the producer.GetID(). This function is needed only for testing
// some condition, like simulate publish error.
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
			msgLen += len(msg.messageBytes) + 8 + 4
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
		return err
	}
	return nil
}

func (producer *Producer) flushUnConfirmedMessages() {
	timeOut := producer.unConfirmed.extractWithTimeOut(time.Duration(0))
	if len(timeOut) > 0 {
		producer.sendConfirmationStatus(timeOut)
	}

}

// GetLastPublishingId returns the last publishing id sent by the producer given the producer name.
// this function is useful when you need to know the last message sent by the producer in case of
// deduplication.
func (producer *Producer) GetLastPublishingId() (int64, error) {
	return producer.options.client.queryPublisherSequence(producer.GetName(), producer.GetStreamName())
}

// Close closes the producer and returns an error if the producer could not be closed.
func (producer *Producer) Close() error {

	return producer.close(Event{
		Command:    CommandDeletePublisher,
		StreamName: producer.GetStreamName(),
		Name:       producer.GetName(),
		Reason:     DeletePublisher,
		Err:        nil,
	})
}
func (producer *Producer) close(reason Event) error {

	if producer.getStatus() == closed {
		return AlreadyClosed
	}

	producer.setStatus(closed)

	reason.StreamName = producer.GetStreamName()
	reason.Name = producer.GetName()
	if producer.closeHandler != nil {
		producer.closeHandler <- reason
		close(producer.closeHandler)
		producer.closeHandler = nil
	}

	producer.stopAndWaitPendingSequencesQueue()

	producer.closeConfirmationStatus()

	if producer.options == nil {
		// the options are usually not nil. This is just for safety and for to make some
		// test easier to write
		logs.LogDebug("producer options is nil, the close will be ignored")
		return nil
	}

	if !producer.options.client.socket.isOpen() {
		return fmt.Errorf("tcp connection is closed")
	}

	// remove from the server only if the producer exists
	if reason.Reason == DeletePublisher {
		_ = producer.options.client.deletePublisher(producer.id)
	}

	_, _ = producer.options.client.coordinator.ExtractProducerById(producer.id)

	if producer.options.client.coordinator.ProducersCount() == 0 {
		_ = producer.options.client.Close()
	}

	if producer.onClose != nil {
		ch := make(chan uint8, 1)
		ch <- producer.id
		producer.onClose(ch)
		close(ch)
	}

	return nil
}

// stopAndWaitPendingSequencesQueue stops the pendingSequencesQueue and waits for the inflight messages to be sent
func (producer *Producer) stopAndWaitPendingSequencesQueue() {

	// Stop the pendingSequencesQueue, so the producer can't send messages anymore
	// but the producer can still handle the inflight messages
	pendingSequences := producer.pendingSequencesQueue.Stop()

	if len(pendingSequences) > 0 {
		// Send as unconfirmed the messages in the pendingSequencesQueue, that have never been sent,
		// with the "entityClosed" error.
		pending := make([]*ConfirmationStatus, 0, len(pendingSequences))
		for _, ps := range pendingSequences {
			cs := &ConfirmationStatus{
				inserted:     time.Now(),
				message:      ps.sourceMsg,
				producerID:   producer.GetID(),
				publishingId: ps.publishingId,
				confirmed:    false,
			}
			cs.updateStatus(entityClosed, false)
			pending = append(pending, cs)
		}
		producer.sendConfirmationStatus(pending)
	}

	// Stop the confirmationTimeoutTicker. It will flush the unconfirmed messages
	producer.confirmationTimeoutTicker.Stop()
	producer.doneTimeoutTicker <- struct{}{}
	close(producer.doneTimeoutTicker)

	// Wait for the inflight messages
	producer.waitForInflightMessages()
	// Close the pendingSequencesQueue. It closes the channel
	producer.pendingSequencesQueue.Close()

}

func (producer *Producer) waitForInflightMessages() {
	// during the close there cloud be pending messages
	// it waits for producer.options.BatchPublishingDelay
	// to flush the last messages
	// see issues/103

	tentatives := 0

	for (producer.unConfirmed.size() > 0) && tentatives < 5 {
		logs.LogInfo("wait inflight messages - unconfirmed len: %d - retry: %d",
			producer.unConfirmed.size(), tentatives)
		producer.flushUnConfirmedMessages()
		time.Sleep(time.Duration(500) * time.Millisecond)
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
		msgLen += len(msg.messageBytes) + 8 + 4
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

	return errWrite.Err
}
