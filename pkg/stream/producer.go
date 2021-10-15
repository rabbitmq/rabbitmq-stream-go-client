package stream

import (
	"bytes"
	"fmt"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/logs"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/message"
	"sync"
	"sync/atomic"
	"time"
)

type UnConfirmedMessage struct {
	Message    message.StreamMessage
	ProducerID uint8
	SequenceID int64
	Confirmed  bool
	Err        error
	LinkedTo   []*UnConfirmedMessage
}

type pendingMessagesSequence struct {
	messages []messageSequence
	size     int
}

type messageSequence struct {
	messageBytes []byte
	size         int
	publishingId int64
}

type Producer struct {
	id                  uint8
	options             *ProducerOptions
	onClose             onInternalClose
	unConfirmedMessages map[int64]*UnConfirmedMessage
	sequence            int64
	mutex               *sync.Mutex
	publishConfirm      chan []*UnConfirmedMessage
	publishError        chan PublishError
	closeHandler        chan Event
	status              int

	/// needed for the async publish
	messageSequenceCh chan messageSequence
	pendingMessages   pendingMessagesSequence
}

type ProducerOptions struct {
	client               *Client
	streamName           string
	Name                 string // Producer name, it is useful to handle deduplication messages
	QueueSize            int    // Internal queue to handle back-pressure, low value reduces the back-pressure on the server
	BatchSize            int    // It is the batch-size aggregation, low value reduce the latency, high value increase the throughput
	BatchPublishingDelay int    // Period to send a batch of messages.
	SubEntrySize         int    // Size of sub Entry, to aggregate more subEntry using one publishing id
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

func NewProducerOptions() *ProducerOptions {
	return &ProducerOptions{
		QueueSize:            defaultQueuePublisherSize,
		BatchSize:            defaultBatchSize,
		BatchPublishingDelay: defaultBatchPublishingDelay,
		SubEntrySize:         1,
	}
}

func (producer *Producer) GetUnConfirmed() map[int64]*UnConfirmedMessage {
	producer.mutex.Lock()
	defer producer.mutex.Unlock()
	return producer.unConfirmedMessages
}

func (producer *Producer) addUnConfirmed(sequence int64, message message.StreamMessage, producerID uint8) {
	producer.mutex.Lock()
	defer producer.mutex.Unlock()
	producer.unConfirmedMessages[sequence] = &UnConfirmedMessage{
		Message:    message,
		ProducerID: producerID,
		SequenceID: sequence,
		Confirmed:  false,
	}
}

func (producer *Producer) removeUnConfirmed(sequence int64) {
	producer.mutex.Lock()
	defer producer.mutex.Unlock()
	delete(producer.unConfirmedMessages, sequence)
}

func (producer *Producer) lenUnConfirmed() int {
	producer.mutex.Lock()
	defer producer.mutex.Unlock()
	return len(producer.unConfirmedMessages)
}

func (producer *Producer) getUnConfirmed(sequence int64) *UnConfirmedMessage {
	producer.mutex.Lock()
	defer producer.mutex.Unlock()
	return producer.unConfirmedMessages[sequence]
}

func (producer *Producer) NotifyPublishConfirmation() ChannelPublishConfirm {
	ch := make(chan []*UnConfirmedMessage)
	producer.publishConfirm = ch
	return ch
}

func (producer *Producer) NotifyPublishError() ChannelPublishError {
	ch := make(chan PublishError, 1)
	producer.publishError = ch
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
						producer.FlushUnConfirmedMessages()
						if producer.publishConfirm != nil {
							close(producer.publishConfirm)
							producer.publishConfirm = nil
						}
						if producer.closeHandler != nil {
							close(producer.closeHandler)
							producer.closeHandler = nil
						}
						return
					}
					if producer.pendingMessages.size+msg.size >= producer.options.client.getTuneState().
						requestedMaxFrameSize {
						producer.sendBufferedMessages()
					}

					producer.pendingMessages.size += msg.size
					producer.pendingMessages.messages = append(producer.pendingMessages.messages, msg)
					if len(producer.pendingMessages.messages) >= (producer.options.BatchSize) {
						producer.sendBufferedMessages()
					}
				}

			case <-ticker.C:
				producer.sendBufferedMessages()
			}

		}
	}(producer.messageSequenceCh)

}

func (producer *Producer) Send(streamMessage message.StreamMessage) error {
	msgBytes, err := streamMessage.MarshalBinary()
	if err != nil {
		return err
	}

	if len(msgBytes)+initBufferPublishSize > producer.options.client.getTuneState().requestedMaxFrameSize {
		return FrameTooLarge
	}

	sequence := producer.getPublishingID(streamMessage)
	producer.addUnConfirmed(sequence, streamMessage, producer.id)

	if producer.getStatus() == open {
		producer.messageSequenceCh <- messageSequence{
			messageBytes: msgBytes,
			size:         len(msgBytes),
			publishingId: sequence,
		}
	} else {
		return fmt.Errorf("producer id: %d  closed", producer.id)
	}

	return nil
}

func (producer *Producer) getPublishingID(message message.StreamMessage) int64 {
	sequence := message.GetPublishingId()
	if message.GetPublishingId() < 0 {
		sequence = atomic.AddInt64(&producer.sequence, 1)
	}
	return sequence
}

func (producer *Producer) BatchSend(batchMessages []message.StreamMessage) error {
	var messagesSequence = make([]messageSequence, len(batchMessages))
	for i, batchMessage := range batchMessages {
		messageBytes, err := batchMessage.MarshalBinary()
		if err != nil {
			return err
		}
		sequence := producer.getPublishingID(batchMessage)
		messagesSequence[i] = messageSequence{
			messageBytes: messageBytes,
			size:         len(messageBytes),
			publishingId: sequence,
		}
		producer.addUnConfirmed(sequence, batchMessage, producer.id)
	}

	return producer.internalBatchSend(messagesSequence)
}

func (producer *Producer) GetID() uint8 {
	return producer.id
}
func (producer *Producer) internalBatchSend(messagesSequence []messageSequence) error {
	return producer.internalBatchSendProdId(messagesSequence, producer.GetID())
}

type subEntry struct {
	messages         []messageSequence
	publishingId     int64
	unCompressedSize int
}
type subEntries = []*subEntry

func (producer *Producer) simpleAggregation(messagesSequence []messageSequence, b *bytes.Buffer) {
	for _, msg := range messagesSequence {
		r := msg.messageBytes
		writeLong(b, msg.publishingId) // publishingId
		writeInt(b, len(r))            // len
		b.Write(r)
	}
}

//unit test for reuse message
//unit test for sub bacthing
func (producer *Producer) subEntryAggregation(aggregation subEntries, b *bytes.Buffer) {
	for _, entry := range aggregation {
		writeLong(b, entry.publishingId)
		writeByte(b, 0x80|0<<4) // 1=SubBatchEntryType:1,CompressionType:3,Reserved:4,
		writeShort(b, int16(len(entry.messages)))
		writeInt(b, entry.unCompressedSize)
		writeInt(b, entry.unCompressedSize)

		for _, msg := range entry.messages {
			r := msg.messageBytes
			writeInt(b, len(r)) // len
			b.Write(r)
		}
	}
}

func (producer *Producer) getSubEntries(msgs []messageSequence, size int) (subEntries, error) {
	aggregation := subEntries{}

	var sub *subEntry
	for _, msg := range msgs {
		if len(aggregation) == 0 || len(sub.messages) >= size {
			sub = &subEntry{}
			sub.publishingId = -1
			aggregation = append(aggregation, sub)
		}
		if sub.publishingId < 0 {
			sub.publishingId = msg.publishingId
		}
		sub.messages = append(sub.messages, msg)
		binary := msg.messageBytes
		sub.unCompressedSize += len(binary) + 4
		if sub.publishingId != msg.publishingId {
			unConfirmed := producer.getUnConfirmed(sub.publishingId)
			if unConfirmed != nil {
				unConfirmed.LinkedTo =
					append(producer.getUnConfirmed(sub.publishingId).LinkedTo,
						producer.getUnConfirmed(msg.publishingId))
			}
		}
	}
	return aggregation, nil
}

/// the producer id is always the producer.GetID(). This function is needed only for testing
// some condition, like simulate publish error, see
func (producer *Producer) internalBatchSendProdId(messagesSequence []messageSequence, producerID uint8) error {
	if producer.getStatus() == closed {
		return fmt.Errorf("producer id: %d closed", producer.id)
	}

	var msgLen int
	var aggregation subEntries

	if producer.options.SubEntrySize > 1 {
		var err error
		aggregation, err = producer.getSubEntries(messagesSequence, producer.options.SubEntrySize)
		if err != nil {
			return err
		}
		msgLen += (8 + 1 + 2 + 4 + 4) * len(aggregation)
	}

	for _, msg := range messagesSequence {
		if producer.options.SubEntrySize == 1 {
			msgLen += msg.size + 8 + 4
		}
		if producer.options.SubEntrySize > 1 {
			msgLen += msg.size + 4
		}
	}

	frameHeaderLength := initBufferPublishSize
	length := frameHeaderLength + msgLen
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	writeProtocolHeader(b, length, commandPublish)
	writeByte(b, producerID)
	numberOfMessages := len(messagesSequence)
	numberOfMessages = numberOfMessages / producer.options.SubEntrySize
	if len(messagesSequence)%producer.options.SubEntrySize != 0 {
		numberOfMessages += 1
	}

	writeInt(b, numberOfMessages) //toExcluded - fromInclude
	if producer.options.SubEntrySize == 1 {
		producer.simpleAggregation(messagesSequence, b)
	}

	if producer.options.SubEntrySize > 1 {
		producer.subEntryAggregation(aggregation, b)
	}

	bufferToWrite := b.Bytes()
	if len(bufferToWrite) > producer.options.client.getTuneState().requestedMaxFrameSize {
		return lookErrorCode(responseCodeFrameTooLarge)
	}

	err := producer.options.client.socket.writeAndFlush(b.Bytes())
	if err != nil {
		// This sleep is need to wait the
		// 800 milliseconds to flush all the pending messages

		producer.setStatus(closed)
		producer.FlushUnConfirmedMessages()
		//time.Sleep(800 * time.Millisecond)
		return err
	}
	return nil
}

func (producer *Producer) FlushUnConfirmedMessages() {
	producer.mutex.Lock()
	if producer.publishConfirm != nil {
		for _, msg := range producer.unConfirmedMessages {
			msg.Confirmed = false
			producer.publishConfirm <- []*UnConfirmedMessage{msg}
			delete(producer.unConfirmedMessages, msg.SequenceID)
		}
	}
	producer.mutex.Unlock()
}

func (producer *Producer) Close() error {
	if producer.getStatus() == closed {
		return AlreadyClosed
	}
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

	close(producer.messageSequenceCh)

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
