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

type Producer struct {
	ID                  uint8
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
	publishChannel chan message.StreamMessage
	queuePublish   []message.StreamMessage
	lastSent       time.Time
}

type ProducerOptions struct {
	client     *Client
	streamName string
	Name       string
	QueueSize  int
	BatchSize  int
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

func NewProducerOptions() *ProducerOptions {
	return &ProducerOptions{
		QueueSize: 1000,
		BatchSize: 100,
	}
}

func (producer *Producer) GetUnConfirmed() map[int64]*UnConfirmedMessage {
	producer.mutex.Lock()
	defer producer.mutex.Unlock()
	return producer.unConfirmedMessages
}

func (producer *Producer) addUnConfirmed(messageid int64, message message.StreamMessage, producerID uint8) {
	producer.mutex.Lock()
	defer producer.mutex.Unlock()
	producer.unConfirmedMessages[messageid] = &UnConfirmedMessage{
		Message:    message,
		ProducerID: producerID,
		SequenceID: messageid,
		Confirmed:  false,
	}
}

func (producer *Producer) removeUnConfirmed(messageid int64) {
	producer.mutex.Lock()
	defer producer.mutex.Unlock()
	delete(producer.unConfirmedMessages, messageid)
}

func (producer *Producer) resetUnConfirmed() {
	producer.mutex.Lock()
	defer producer.mutex.Unlock()
	producer.unConfirmedMessages = map[int64]*UnConfirmedMessage{}
}

func (producer *Producer) lenUnConfirmed() int {
	producer.mutex.Lock()
	defer producer.mutex.Unlock()
	return len(producer.unConfirmedMessages)
}

func (producer *Producer) getUnConfirmed(messageid int64) *UnConfirmedMessage {
	producer.mutex.Lock()
	defer producer.mutex.Unlock()
	return producer.unConfirmedMessages[messageid]
}

func (producer *Producer) NotifyPublishConfirmation() ChannelPublishConfirm {
	ch := make(chan []*UnConfirmedMessage, 1)
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
	if len(producer.queuePublish) > producer.options.BatchSize {
		logs.LogError("eeeeeeeeeeeee")
	}

	if len(producer.queuePublish) > 0 {
		//logs.LogInfo("len %d",  len(producer.queuePublish))
		producer.BatchPublish(producer.queuePublish)
		producer.queuePublish = producer.queuePublish[:0]
		producer.lastSent = time.Now()
	}
}
func (producer *Producer) startPublishTask() {
	producer.lastSent = time.Now()

	go func() {
		var ticker = time.NewTicker(200 * time.Millisecond)
		defer ticker.Stop()
		for {

			select {
			case msg, running := <-producer.publishChannel:
				if !running {
					return
				}
				producer.queuePublish = append(producer.queuePublish, msg)
				if len(producer.queuePublish) >= producer.options.BatchSize {
					producer.sendBufferedMessages()
				}

			case <-ticker.C:
				producer.sendBufferedMessages()
			}

		}
	}()
}

func (producer *Producer) Publish(message message.StreamMessage) error {
	if producer.getStatus() == closed {
		return fmt.Errorf("producer id: %d  already closed", producer.ID)
	}
	producer.publishChannel <- message

	return nil
}

func (producer *Producer) BatchPublish(batchMessages []message.StreamMessage) error {
	if producer.getStatus() == closed {
		return fmt.Errorf("producer id: %d  already closed", producer.ID)
	}

	if len(batchMessages) > 1000 {
		return fmt.Errorf("%d - %s", len(batchMessages), "too many messages")
	}
	var msgLen int
	var sequences = make([]int64, len(batchMessages))
	for idx, msg := range batchMessages {
		r, _ := msg.MarshalBinary()
		msgLen += len(r) + 8 + 4
		if msg.GetPublishingId() >= 0 {
			/// The user didn't set the id
			/// so, it will be automatic assigned by the client
			sequences[idx] = msg.GetPublishingId()
		} else {
			sequences[idx] = atomic.AddInt64(&producer.sequence, 1)
		}

		producer.addUnConfirmed(sequences[idx], msg, producer.ID)
	}

	frameHeaderLength := 2 + 2 + 1 + 4
	length := frameHeaderLength + msgLen
	publishId := producer.ID
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	writeProtocolHeader(b, length, commandPublish)
	writeByte(b, publishId)
	writeInt(b, len(batchMessages)) //toExcluded - fromInclude

	for idx, msg := range batchMessages {
		r, _ := msg.MarshalBinary()
		writeLong(b, sequences[idx]) // sequence
		writeInt(b, len(r))          // len
		b.Write(r)
	}

	bufferToWrite := b.Bytes()
	if len(bufferToWrite) > producer.options.client.tuneState.requestedMaxFrameSize {
		return lookErrorCode(responseCodeFrameTooLarge)
	}

	err := producer.options.client.socket.writeAndFlush(b.Bytes())
	// TODO handle the socket read error to close the producer
	if err != nil {
		if producer.publishConfirm != nil {
			var unConfirmedMessages []*UnConfirmedMessage
			for idx, msg := range batchMessages {
				unConfirmedMessages = append(unConfirmedMessages, &UnConfirmedMessage{
					Message:    msg,
					ProducerID: producer.ID,
					SequenceID: sequences[idx],
					Confirmed:  false,
					Err:        err,
				})
				producer.removeUnConfirmed(msg.GetPublishingId())
			}
			producer.publishConfirm <- unConfirmedMessages
		}

		return err
	}
	return nil
}

func (producer *Producer) Close() error {
	producer.setStatus(closed)
	close(producer.publishChannel)
	if !producer.options.client.socket.isOpen() {
		return fmt.Errorf("connection already closed")
	}

	err := producer.options.client.deletePublisher(producer.ID)
	if err != nil {
		return err
	}
	if producer.options.client.coordinator.ProducersCount() == 0 {
		err := producer.options.client.Close()
		if err != nil {
			return err
		}
	}
	ch := make(chan uint8, 1)
	ch <- producer.ID
	producer.onClose(ch)
	close(ch)

	producer.mutex.Lock()
	if producer.publishConfirm != nil {
		close(producer.publishConfirm)
		producer.publishConfirm = nil
	}
	if producer.closeHandler != nil {
		close(producer.closeHandler)
		producer.closeHandler = nil
	}

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

	//producer, _ := c.coordinator.GetProducerById(publisherId)
	// if there are UnConfirmed messages here, most likely there will be an
	// publisher error. Just try to wait a bit to receive the call back

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
