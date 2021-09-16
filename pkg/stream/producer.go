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
}

type subEntryMessage struct {
	messages []messageSequence
	//size     int
}

type pendingMessagesSequence struct {
	subEntry []subEntryMessage
	size     int
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
	pendingMessages   pendingMessagesSequence
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
		err := producer.internalBatchSend(producer.pendingMessages.subEntry)
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
					if producer.pendingMessages.size+msg.size >= producer.options.client.getTuneState().requestedMaxFrameSize {
						producer.sendBufferedMessages()
					}

					producer.pendingMessages.size += producer.pendingMessages.size + msg.size

					producer.pendingMessages.subEntry = append(producer.pendingMessages.subEntry,
						subEntryMessage{messages: make([]messageSequence, 1)})
					producer.pendingMessages.subEntry[len(producer.pendingMessages.subEntry)-1].messages[0] = msg

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
	sequence := producer.getPublishingID(streamMessage)
	producer.addUnConfirmed(sequence, streamMessage, producer.id)

	if producer.getStatus() == closed {
		return fmt.Errorf("producer id: %d  closed", producer.id)
	}

	producer.messageSequenceCh <- messageSequence{
		message:      streamMessage,
		size:         len(msgBytes),
		publishingId: sequence,
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
	var subEntry = make([]subEntryMessage, len(batchMessages))

	for i, batchMessage := range batchMessages {
		messageBytes, err := batchMessage.MarshalBinary()
		if err != nil {
			return err
		}
		sequence := producer.getPublishingID(batchMessage)
		messageSeq := messageSequence{
			message:      batchMessage,
			size:         len(messageBytes),
			publishingId: sequence,
		}
		producer.addUnConfirmed(sequence, batchMessage, producer.id)
		subEntry[i] = subEntryMessage{messages: []messageSequence{messageSeq}}
	}

	return producer.internalBatchSend(subEntry)
}

func (producer *Producer) GetID() uint8 {
	return producer.id
}
func (producer *Producer) internalBatchSend(subEntry []subEntryMessage) error {
	return producer.internalBatchSendProdId(subEntry, producer.GetID())
}

/// the producer id is always the producer.GetID(). This function is needed only for testing
// some condition, like simulate publish error, see
func (producer *Producer) internalBatchSendProdId(subEntry []subEntryMessage, producerID uint8) error {
	if producer.getStatus() == closed {
		return fmt.Errorf("producer id: %d closed", producer.id)
	}

	var msgLen int
	var numberOfMessages = 0
	for _, entryMessage := range subEntry {
		numberOfMessages += len(entryMessage.messages)
		for _, msg := range entryMessage.messages {
			msgLen += msg.size + 8 + 4
		}
	}

	frameHeaderLength := initBufferPublishSize
	length := frameHeaderLength + msgLen
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	writeProtocolHeader(b, length, commandPublish)
	writeByte(b, producerID)
	writeInt(b, numberOfMessages) //toExcluded - fromInclude

	for _, entryMessage := range subEntry {
		for _, msg := range entryMessage.messages {
			r, _ := msg.message.MarshalBinary()
			writeLong(b, msg.publishingId) // publishingId
			writeInt(b, len(r))            // len
			b.Write(r)

		}
	}

	bufferToWrite := b.Bytes()
	if len(bufferToWrite) > producer.options.client.getTuneState().requestedMaxFrameSize {
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
