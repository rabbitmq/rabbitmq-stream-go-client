package stream

import (
	"bytes"
	"fmt"
	"github.com/pkg/errors"
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
}

// subEntryMessage keeps the relation between the producer.options.SubEntrySize
// and messages
type subEntryMessage struct {
	messages []*messageSequence
}

// the accumulatedEntity is meant to keep the structure sub-entry -> messages
// in case of simple publish will be 1 sub entry 1 message
// see the comment on (producer *Producer) startPublishTask()
type accumulatedEntity struct {
	subEntry []subEntryMessage
	size     int
}

func (p *accumulatedEntity) batchSize(messages []*messageSequence) int {
	return len(messages)
}

func (p *accumulatedEntity) uncompressedSizeInBytes(messages []*messageSequence) int {
	result := 0
	for _, sequence := range messages {
		binary, err := sequence.message.MarshalBinary()
		result += len(binary) + 4 // int to write to the buffer
		if err != nil {
			return 0
		}

	}
	return result
}

func (p *accumulatedEntity) maybeAddSubEntry(producerSubEntrySize int) {
	if len(p.subEntry) == 0 {
		p.subEntry = append(p.subEntry,
			subEntryMessage{messages: make([]*messageSequence, 0)})

	} else {
		if len(p.subEntry[len(p.subEntry)-1].messages) >= producerSubEntrySize {
			p.subEntry = append(p.subEntry,
				subEntryMessage{messages: make([]*messageSequence, 0)})
		}
	}
}

/// addMessage positions the message in the right position
/// given the subEntry value. By default, the relation is 1 sub-entry 1 message
func (p *accumulatedEntity) addMessage(producerSubEntrySize int, streamMessage *messageSequence) {
	p.maybeAddSubEntry(producerSubEntrySize)
	l := len(p.subEntry) - 1
	p.subEntry[l].messages = append(p.subEntry[l].messages, streamMessage)
	p.size += streamMessage.size
}

type messageSequence struct {
	message      message.StreamMessage
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
	pendingMessages   accumulatedEntity
}

type ProducerOptions struct {
	client               *Client
	streamName           string
	Name                 string // Producer name, it is useful to handle deduplication subEntry
	QueueSize            int    // Internal queue to handle back-pressure, low value reduces the back-pressure on the server
	BatchSize            int    // It is the batch-size aggregation, low value reduce the latency, high value increase the throughput
	BatchPublishingDelay int    // Period to send a batch of subEntry.
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

func (producer *Producer) addUnConfirmed(sequence int64, streamMessage message.StreamMessage, producerID uint8) {
	producer.mutex.Lock()
	defer producer.mutex.Unlock()

	producer.unConfirmedMessages[sequence] = &UnConfirmedMessage{
		Message:    streamMessage,
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

	if len(producer.pendingMessages.subEntry) > 0 {
		err := producer.internalBatchSend(producer.pendingMessages)
		if err != nil {
			return
		}
		producer.pendingMessages.subEntry = producer.pendingMessages.subEntry[:0]
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
						return
					}
					// Checks if with the next message the frame size is still valid
					// else we send the buffered messages
					if producer.pendingMessages.size+msg.size >= producer.options.client.getTuneState().requestedMaxFrameSize {
						producer.sendBufferedMessages()
					}

					// in case SubEntrySize = 1 means simple publish
					// SubEntrySize > 0 means subBatch publish
					// for example sub entry = 5 and batch size = 10
					// batch size * sub entry = 50 messages 10 publishing id

					// sub entry = 10.000 > Frame size raise an error can't split 10.000 5.000 5.000
					// batch size = 5
					// 10.000 * 5 = 50.0000 5 publishing
					// the accumulatedEntity is meant to keep the structure sub-entry -> messages
					// in case of simple publish will be 1 sub entry 1 message
					producer.pendingMessages.addMessage(producer.options.SubEntrySize, &msg)
					producer.addUnConfirmed(producer.applyPublishingIdAsLong(&msg), msg.message, producer.id)

					/// as batch size count the number of the subEntry
					if len(producer.pendingMessages.subEntry) >= producer.options.BatchSize {
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

	msg := messageSequence{
		message: streamMessage,
		size:    len(msgBytes),
	}

	if producer.getStatus() == closed {
		if producer.publishConfirm != nil {
			unConfirmedMessage := &UnConfirmedMessage{
				Message:    streamMessage,
				ProducerID: producer.id,
				SequenceID: streamMessage.GetPublishingId(),
				Confirmed:  false,
				Err:        fmt.Errorf("producer id: %d  closed", producer.id),
			}
			producer.publishConfirm <- []*UnConfirmedMessage{unConfirmedMessage}
		}

		return fmt.Errorf("producer id: %d  closed", producer.id)
	}

	producer.messageSequenceCh <- msg

	return nil
}

func (producer *Producer) applyPublishingIdAsLong(messageSequence *messageSequence) int64 {
	sequence := messageSequence.message.GetPublishingId()
	if messageSequence.message.GetPublishingId() < 0 {
		sequence = atomic.AddInt64(&producer.sequence, 1)
	}
	messageSequence.publishingId = sequence

	return sequence
}

func (producer *Producer) BatchSend(batchMessages []message.StreamMessage) error {
	if producer.options.SubEntrySize > 1 {
		return errors.New("Can't use BatchSend with SubEntrySize > 1")
	}
	var subEntry = make([]subEntryMessage, len(batchMessages))

	for i, batchMessage := range batchMessages {
		messageBytes, err := batchMessage.MarshalBinary()
		if err != nil {
			return err
		}
		messageSeq := messageSequence{
			message: batchMessage,
			size:    len(messageBytes),
		}
		sequence := producer.applyPublishingIdAsLong(&messageSeq)

		producer.addUnConfirmed(sequence, batchMessage, producer.id)
		subEntry[i] = subEntryMessage{messages: []*messageSequence{&messageSeq}}
	}
	messages := accumulatedEntity{
		subEntry: subEntry,
		size:     0,
	}
	return producer.internalBatchSend(messages)
}

func (producer *Producer) GetID() uint8 {
	return producer.id
}
func (producer *Producer) internalBatchSend(pendingMessages accumulatedEntity) error {
	return producer.internalBatchSendProdId(pendingMessages, producer.GetID())
}

/// the producer id is always the producer.GetID(). This function is needed only for testing
// some condition, like simulate publish error, see
func (producer *Producer) internalBatchSendProdId(pendingMessages accumulatedEntity, producerID uint8) error {
	if producer.getStatus() == closed {
		return fmt.Errorf("producer id: %d closed", producer.id)
	}

	var msgLen int
	var numberOfMessages = len(pendingMessages.subEntry)
	for _, entryMessage := range pendingMessages.subEntry {
		if producer.options.SubEntrySize > 1 {
			msgLen += 8 + 1 + 2 + 4 + 4
		}
		for _, msg := range entryMessage.messages {
			if producer.options.SubEntrySize == 1 {
				msgLen += msg.size + 8 + 4
			}
			if producer.options.SubEntrySize > 1 {
				msgLen += msg.size + 4
			}
		}
	}

	frameHeaderLength := initBufferPublishSize
	length := frameHeaderLength + msgLen
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	writeProtocolHeader(b, length, commandPublish)
	writeByte(b, producerID)
	writeInt(b, numberOfMessages) //toExcluded - fromInclude
	// SubEntrySize == 1 in this case the publishing is standard, no sub-batching
	if producer.options.SubEntrySize == 1 {
		producer.simpleAggregation(pendingMessages, b)
	}

	if producer.options.SubEntrySize > 1 {
		producer.subEntryAggregation(pendingMessages, b)
	}

	bufferToWrite := b.Bytes()
	if len(bufferToWrite) > producer.options.client.getTuneState().requestedMaxFrameSize {
		producer.FlushUnConfirmedMessages()
		return lookErrorCode(responseCodeFrameTooLarge)
	}

	err := producer.options.client.socket.writeAndFlush(b.Bytes())
	if err != nil {
		// This sleep is need to wait the
		// 800 milliseconds to flush all the pending subEntry
		time.Sleep(800 * time.Millisecond)
		producer.setStatus(closed)
		producer.FlushUnConfirmedMessages()

		return err
	}
	return nil
}

func (producer *Producer) simpleAggregation(pendingMessages accumulatedEntity, b *bytes.Buffer) {
	for _, entryMessage := range pendingMessages.subEntry {
		for _, msg := range entryMessage.messages {
			r, _ := msg.message.MarshalBinary()
			writeLong(b, msg.publishingId) // publishingId
			writeInt(b, len(r))            // len
			b.Write(r)

		}
	}
}

func (producer *Producer) subEntryAggregation(pendingMessages accumulatedEntity, b *bytes.Buffer) {

	for _, entryMessage := range pendingMessages.subEntry {
		//return batchToPublish.batchSize();
		//batchToPublish.write(bb);
		//bb.writeInt(batchToPublish.sizeInBytes());
		//bb.writeInt(batchToPublish.uncompressedSizeInBytes());
		//EncodedMessageBatch batchToPublish = (EncodedMessageBatch) entity;
		writeLong(b, entryMessage.messages[0].publishingId)
		writeByte(b, 0x80|0<<4) // 1=SubBatchEntryType:1,CompressionType:3,Reserved:4,
		writeShort(b, int16(pendingMessages.batchSize(entryMessage.messages)))
		sizeCompress := pendingMessages.uncompressedSizeInBytes(entryMessage.messages)
		writeInt(b, sizeCompress)
		writeInt(b, sizeCompress)

		for _, msg := range entryMessage.messages {
			r, _ := msg.message.MarshalBinary()
			writeInt(b, len(r)) // len
			b.Write(r)
		}
	}
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
		return err
	}
	if producer.options.client.coordinator.ProducersCount() == 0 {
		err := producer.options.client.Close()
		if err != nil {
			return err
		}
	}

	if producer.onClose != nil {
		ch := make(chan uint8, 1)
		ch <- producer.id
		producer.onClose(ch)
		close(ch)
	}

	producer.mutex.Lock()
	if producer.publishConfirm != nil {
		close(producer.publishConfirm)
		producer.publishConfirm = nil
	}
	if producer.closeHandler != nil {
		close(producer.closeHandler)
		producer.closeHandler = nil
	}
	close(producer.messageSequenceCh)

	producer.mutex.Unlock()

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
