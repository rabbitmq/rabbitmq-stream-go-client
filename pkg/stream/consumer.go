package stream

import (
	"bytes"
	"fmt"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	logs "github.com/rabbitmq/rabbitmq-stream-go-client/pkg/logs"
	"sync"
	"time"
)

type Consumer struct {
	ID              uint8
	response        *Response
	options         *ConsumerOptions
	onClose         onInternalClose
	mutex           *sync.Mutex
	MessagesHandler MessagesHandler
	// different form ConsumerOptions.offset. ConsumerOptions.offset is just the configuration
	// and won't change. currentOffset is the status of the offset
	currentOffset int64

	// Remembers the last stored offset (manual or automatic) to avoid to store always the same values
	lastStoredOffset int64

	closeHandler chan Event
	// see autocommit strategy
	// it is needed to trigger the
	// auto-commit after messageCountBeforeStorage
	messageCountBeforeStorage int

	status int

	// Single Active consumer. The consumer can be running
	// but not active. This flag is used to know if the consumer
	// is in waiting mode or not.
	// in normal mode, the consumer is always isPromotedAsActive==true
	isPromotedAsActive bool

	// lastAutoCommitStored tracks when the offset was last flushed
	lastAutoCommitStored time.Time
}

func (consumer *Consumer) setStatus(status int) {
	consumer.mutex.Lock()
	defer consumer.mutex.Unlock()
	consumer.status = status
}

func (consumer *Consumer) getStatus() int {
	consumer.mutex.Lock()
	defer consumer.mutex.Unlock()
	return consumer.status
}

func (consumer *Consumer) GetStreamName() string {
	if consumer.options == nil {
		return ""
	}
	return consumer.options.streamName
}

func (consumer *Consumer) GetName() string {
	if consumer.options == nil {
		return ""
	}
	return consumer.options.ConsumerName
}

func (consumer *Consumer) setCurrentOffset(offset int64) {
	consumer.mutex.Lock()
	defer consumer.mutex.Unlock()
	consumer.currentOffset = offset
}

func (consumer *Consumer) GetOffset() int64 {
	consumer.mutex.Lock()
	defer consumer.mutex.Unlock()
	return consumer.currentOffset
}

// isActive returns true if the consumer is promoted as active
// used for Single Active Consumer. Always true in other cases
func (consumer *Consumer) isActive() bool {
	consumer.mutex.Lock()
	defer consumer.mutex.Unlock()
	return consumer.isPromotedAsActive
}

func (consumer *Consumer) setPromotedAsActive(promoted bool) {
	consumer.mutex.Lock()
	defer consumer.mutex.Unlock()
	consumer.isPromotedAsActive = promoted

}

func (consumer *Consumer) GetLastStoredOffset() int64 {
	consumer.mutex.Lock()
	defer consumer.mutex.Unlock()
	return consumer.lastStoredOffset
}

func (consumer *Consumer) updateLastStoredOffset() bool {
	consumer.mutex.Lock()
	defer consumer.mutex.Unlock()
	if consumer.lastStoredOffset < consumer.currentOffset {
		consumer.lastStoredOffset = consumer.currentOffset
		return true
	}
	return false
}

func (consumer *Consumer) GetCloseHandler() chan Event {
	consumer.mutex.Lock()
	defer consumer.mutex.Unlock()
	return consumer.closeHandler
}

func (consumer *Consumer) NotifyClose() ChannelClose {
	consumer.mutex.Lock()
	defer consumer.mutex.Unlock()
	ch := make(chan Event, 1)
	consumer.closeHandler = ch
	return ch
}

type ConsumerContext struct {
	Consumer  *Consumer
	chunkInfo *chunkInfo
}

func (cc ConsumerContext) GetEntriesCount() uint16 {
	return cc.chunkInfo.numEntries
}

type MessagesHandler func(consumerContext ConsumerContext, message *amqp.Message)

type AutoCommitStrategy struct {
	messageCountBeforeStorage int
	flushInterval             time.Duration
}

func (ac *AutoCommitStrategy) SetCountBeforeStorage(messageCountBeforeStorage int) *AutoCommitStrategy {
	ac.messageCountBeforeStorage = messageCountBeforeStorage
	return ac
}
func (ac *AutoCommitStrategy) SetFlushInterval(flushInterval time.Duration) *AutoCommitStrategy {
	ac.flushInterval = flushInterval
	return ac
}

func NewAutoCommitStrategy() *AutoCommitStrategy {
	return &AutoCommitStrategy{
		messageCountBeforeStorage: 10_000,
		flushInterval:             5 * time.Second,
	}
}

type PostFilter func(message *amqp.Message) bool

type ConsumerFilter struct {
	Values          []string
	MatchUnfiltered bool
	PostFilter      PostFilter
}

func NewConsumerFilter(values []string, matchUnfiltered bool, postFilter PostFilter) *ConsumerFilter {
	return &ConsumerFilter{
		Values:          values,
		MatchUnfiltered: matchUnfiltered,
		PostFilter:      postFilter,
	}
}

type ConsumerUpdate func(streamName string, isActive bool) OffsetSpecification

type SingleActiveConsumer struct {
	Enabled bool
	// ConsumerUpdate is the function that will be called when the consumer is promoted
	// that is when the consumer is active. The function will receive a boolean that is true
	// the user can decide to return a new offset to start from.
	ConsumerUpdate ConsumerUpdate
	// This offset is the one form the user that decides from the ConsumerUpdate function
	// nothing to do with: ConsumerOptions.Offset
	// the ConsumerOptions.Offset is the initial offset and in case of SingleActiveConsumer
	// is not used because the consumer will be promoted and the offset will be set by the ConsumerUpdate
	// This is needed to filter the messages during the promotion where needed
	offsetSpecification OffsetSpecification

	// SingleActiveConsumer can be used with the super stream consumer
	// in this case we need to pass the super stream name
	superStream string
}

func NewSingleActiveConsumer(ConsumerUpdate ConsumerUpdate) *SingleActiveConsumer {
	return &SingleActiveConsumer{
		Enabled:        true,
		ConsumerUpdate: ConsumerUpdate,
	}
}

func newSingleActiveConsumerWithAllParameters(
	ConsumerUpdate ConsumerUpdate, isEnabled bool, superStream string) *SingleActiveConsumer {
	return &SingleActiveConsumer{
		Enabled:        isEnabled,
		ConsumerUpdate: ConsumerUpdate,
		superStream:    superStream,
	}
}

func (s *SingleActiveConsumer) SetEnabled(enabled bool) *SingleActiveConsumer {
	s.Enabled = enabled
	return s
}

type ConsumerOptions struct {
	client               *Client
	ConsumerName         string
	streamName           string
	autocommit           bool
	autoCommitStrategy   *AutoCommitStrategy
	Offset               OffsetSpecification
	CRCCheck             bool
	initialCredits       int16
	ClientProvidedName   string
	Filter               *ConsumerFilter
	SingleActiveConsumer *SingleActiveConsumer
}

func NewConsumerOptions() *ConsumerOptions {
	return &ConsumerOptions{
		Offset:             OffsetSpecification{}.Last(),
		autocommit:         false,
		autoCommitStrategy: NewAutoCommitStrategy(),
		CRCCheck:           false,
		initialCredits:     10,
		ClientProvidedName: "go-stream-consumer",
		Filter:             nil,
	}
}

func (c *ConsumerOptions) SetConsumerName(consumerName string) *ConsumerOptions {
	c.ConsumerName = consumerName
	return c
}

func (c *ConsumerOptions) SetCRCCheck(CRCCheck bool) *ConsumerOptions {
	c.CRCCheck = CRCCheck
	return c
}

func (c *ConsumerOptions) SetInitialCredits(initialCredits int16) *ConsumerOptions {
	c.initialCredits = initialCredits
	return c
}

func (c *ConsumerOptions) SetAutoCommit(autoCommitStrategy *AutoCommitStrategy) *ConsumerOptions {
	c.autocommit = true
	if autoCommitStrategy == nil {
		c.autoCommitStrategy = NewAutoCommitStrategy()
	} else {
		c.autoCommitStrategy = autoCommitStrategy
	}
	return c
}

func (c *ConsumerOptions) SetManualCommit() *ConsumerOptions {
	c.autocommit = false
	return c
}
func (c *ConsumerOptions) SetOffset(offset OffsetSpecification) *ConsumerOptions {
	c.Offset = offset
	return c
}

func (c *ConsumerOptions) SetClientProvidedName(clientProvidedName string) *ConsumerOptions {
	c.ClientProvidedName = clientProvidedName
	return c
}

func (c *ConsumerOptions) SetFilter(filter *ConsumerFilter) *ConsumerOptions {
	c.Filter = filter
	return c
}

func (c *ConsumerOptions) SetSingleActiveConsumer(singleActiveConsumer *SingleActiveConsumer) *ConsumerOptions {
	c.SingleActiveConsumer = singleActiveConsumer
	return c
}

func (c *ConsumerOptions) IsSingleActiveConsumerEnabled() bool {
	return c.SingleActiveConsumer != nil && c.SingleActiveConsumer.Enabled
}

func (c *ConsumerOptions) IsFilterEnabled() bool {
	return c.Filter != nil
}

func (c *Client) credit(subscriptionId byte, credit int16) {
	length := 2 + 2 + 1 + 2
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	writeProtocolHeader(b, length, commandCredit)
	writeByte(b, subscriptionId)
	writeShort(b, credit)
	err := c.socket.writeAndFlush(b.Bytes())
	if err != nil {
		logs.LogWarn("credit error:%s", err)
	}
}

func (consumer *Consumer) Close() error {
	if consumer.getStatus() == closed {
		return AlreadyClosed
	}
	consumer.cacheStoreOffset()

	consumer.setStatus(closed)
	_, errGet := consumer.options.client.coordinator.GetConsumerById(consumer.ID)
	if errGet != nil {
		return nil
	}

	length := 2 + 2 + 4 + 1
	resp := consumer.options.client.coordinator.NewResponse(CommandUnsubscribe)
	correlationId := resp.correlationid
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	writeProtocolHeader(b, length, CommandUnsubscribe,
		correlationId)

	writeByte(b, consumer.ID)
	err := consumer.options.client.handleWrite(b.Bytes(), resp)
	if err.Err != nil && err.isTimeout {
		return err.Err
	}
	consumer.response.code <- Code{id: closeChannel}
	errC := consumer.options.client.coordinator.RemoveConsumerById(consumer.ID, Event{
		Command:    CommandUnsubscribe,
		StreamName: consumer.GetStreamName(),
		Name:       consumer.GetName(),
		Reason:     "unSubscribe",
		Err:        nil,
	})

	if errC != nil {
		logs.LogWarn("Error during remove consumer id:%s", errC)

	}

	if consumer.options.client.coordinator.ConsumersCount() == 0 {
		err := consumer.options.client.Close()
		if err != nil {
			return err
		}
	}

	ch := make(chan uint8, 1)
	ch <- consumer.ID
	consumer.onClose(ch)
	close(ch)
	return err.Err
}

func (consumer *Consumer) cacheStoreOffset() {
	if consumer.options.autocommit {
		consumer.mutex.Lock()
		consumer.lastAutoCommitStored = time.Now()
		consumer.messageCountBeforeStorage = 0
		consumer.mutex.Unlock() // updateLastStoredOffset() in internalStoreOffset() also locks mutex, so not using defer for unlock

		err := consumer.internalStoreOffset()
		if err != nil {
			logs.LogError("cache Store Offset error : %s", err)
		}
	}
}

func (consumer *Consumer) increaseMessageCountBeforeStorage() int {
	consumer.mutex.Lock()
	defer consumer.mutex.Unlock()
	consumer.messageCountBeforeStorage += 1
	return consumer.messageCountBeforeStorage
}

func (consumer *Consumer) getLastAutoCommitStored() time.Time {
	consumer.mutex.Lock()
	defer consumer.mutex.Unlock()
	return consumer.lastAutoCommitStored
}

func (consumer *Consumer) StoreOffset() error {
	return consumer.internalStoreOffset()
}
func (consumer *Consumer) StoreCustomOffset(offset int64) error {
	consumer.mutex.Lock()
	defer consumer.mutex.Unlock()

	if consumer.lastStoredOffset < offset {
		consumer.lastStoredOffset = offset
		return consumer.writeOffsetToSocket(offset)
	}
	return nil
}
func (consumer *Consumer) internalStoreOffset() error {
	if consumer.options.streamName == "" {
		return fmt.Errorf("stream Name can't be empty")
	}

	if consumer.updateLastStoredOffset() {
		return consumer.writeOffsetToSocket(consumer.GetOffset())
	}
	return nil
}
func (consumer *Consumer) writeOffsetToSocket(offset int64) error {
	length := 2 + 2 + 2 + len(consumer.options.ConsumerName) + 2 +
		len(consumer.options.streamName) + 8
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	writeProtocolHeader(b, length, commandStoreOffset)

	writeString(b, consumer.options.ConsumerName)
	writeString(b, consumer.options.streamName)

	writeLong(b, offset)
	return consumer.options.client.socket.writeAndFlush(b.Bytes())
}

func (consumer *Consumer) writeConsumeUpdateOffsetToSocket(correlationID uint32, offsetSpec OffsetSpecification) error {
	length := 2 + 2 + 4 + 2 + 2
	if offsetSpec.isOffset() ||
		offsetSpec.isTimestamp() {
		length += 8
	}

	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	writeProtocolHeader(b, length, commandConsumerUpdate)

	writeUInt(b, correlationID)
	writeUShort(b, responseCodeOk)

	writeShort(b, offsetSpec.typeOfs)

	if offsetSpec.isOffset() ||
		offsetSpec.isTimestamp() {
		writeLong(b, offsetSpec.offset)
	}
	return consumer.options.client.socket.writeAndFlush(b.Bytes())
}

func (consumer *Consumer) QueryOffset() (int64, error) {
	return consumer.options.client.queryOffset(consumer.options.ConsumerName, consumer.options.streamName)
}

/*
SetOffset constants
*/
const (
	typeFirst     = int16(1)
	typeLast      = int16(2)
	typeNext      = int16(3)
	typeOffset    = int16(4)
	typeTimestamp = int16(5)
	// Deprecated: see LastConsumed()
	typeLastConsumed = int16(6)
)

type OffsetSpecification struct {
	typeOfs int16
	offset  int64
}

func (o OffsetSpecification) First() OffsetSpecification {
	o.typeOfs = typeFirst
	return o
}

func (o OffsetSpecification) Last() OffsetSpecification {
	o.typeOfs = typeLast
	return o
}

func (o OffsetSpecification) Next() OffsetSpecification {
	o.typeOfs = typeNext
	return o
}

func (o OffsetSpecification) Offset(offset int64) OffsetSpecification {
	o.typeOfs = typeOffset
	o.offset = offset
	return o
}

func (o OffsetSpecification) Timestamp(offset int64) OffsetSpecification {
	o.typeOfs = typeTimestamp
	o.offset = offset
	return o
}

func (o OffsetSpecification) isOffset() bool {
	return o.typeOfs == typeOffset || o.typeOfs == typeLastConsumed
}

// Deprecated: see LastConsumed()
func (o OffsetSpecification) isLastConsumed() bool {
	return o.typeOfs == typeLastConsumed
}
func (o OffsetSpecification) isTimestamp() bool {
	return o.typeOfs == typeTimestamp
}

// Deprecated: The method name may be misleading.
// The method does not indicate the last message consumed of the stream but the last stored offset.
// The method was added to help the user, but it created confusion.
// Use `QueryOffset` instead.:
//
//		offset, err := env.QueryOffset(consumerName, streamName)
//	 // check the error
//	 ....
//	 SetOffset(stream.OffsetSpecification{}.Offset(offset)).
//
// So in this way it possible to start from the last offset stored and customize the behavior
func (o OffsetSpecification) LastConsumed() OffsetSpecification {
	o.typeOfs = typeLastConsumed
	o.offset = -1
	return o
}

func (o OffsetSpecification) String() string {
	switch o.typeOfs {
	case typeFirst:
		return "first"
	case typeNext:
		return "next"
	case typeLast:
		return "last"
	case typeLastConsumed:
		return "last consumed"
	case typeOffset:
		return fmt.Sprintf("%s, value: %d", "offset", o.offset)
	case typeTimestamp:
		return fmt.Sprintf("%s, value: %d", "time-stamp", o.offset)
	}
	return ""
}
