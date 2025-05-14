package stream

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"strconv"
	"sync"
	"time"
)

type Coordinator struct {
	counter          int
	producers        *sync.Map
	consumers        *sync.Map
	responses        map[interface{}]interface{}
	nextItemProducer uint8
	nextItemConsumer uint8
	mutex            *sync.Mutex
}

type Code struct {
	id uint16
}

type offsetMessage struct {
	message *amqp.Message
	offset  int64
}

type offsetMessages = []*offsetMessage
type chunkInfo struct {
	offsetMessages offsetMessages
	numEntries     uint16
}

type Response struct {
	code               chan Code
	data               chan interface{}
	commandDescription string
	correlationid      int
}

func NewCoordinator() *Coordinator {
	return &Coordinator{mutex: &sync.Mutex{},
		producers: &sync.Map{},
		consumers: &sync.Map{},
		responses: make(map[interface{}]interface{})}
}

// producersEnvironment
func (coordinator *Coordinator) NewProducer(
	parameters *ProducerOptions, cleanUp func()) (*Producer, error) {
	coordinator.mutex.Lock()
	defer coordinator.mutex.Unlock()
	dynSize := 10000
	queueSize := defaultQueuePublisherSize
	tickerTime := defaultConfirmationTimeOut
	if parameters != nil {
		dynSize = parameters.BatchSize
		tickerTime = parameters.ConfirmationTimeOut
		queueSize = parameters.QueueSize
	}

	var lastId, err = coordinator.getNextProducerItem()
	if err != nil {
		return nil, err
	}
	var producer = &Producer{id: lastId,
		options:                   parameters,
		mutex:                     &sync.RWMutex{},
		unConfirmed:               newUnConfirmed(queueSize),
		confirmationTimeoutTicker: time.NewTicker(tickerTime),
		doneTimeoutTicker:         make(chan struct{}, 1),
		status:                    open,
		pendingSequencesQueue:     NewBlockingQueue[*messageSequence](dynSize),
		confirmMutex:              &sync.Mutex{},
		onClose:                   cleanUp,
	}
	coordinator.producers.Store(lastId, producer)
	return producer, err
}

func (coordinator *Coordinator) RemoveConsumerById(id interface{}, reason Event) error {
	consumer, err := coordinator.ExtractConsumerById(id)
	if err != nil {
		return err
	}
	return consumer.close(reason)

}
func (coordinator *Coordinator) Consumers() *sync.Map {
	return coordinator.consumers
}

func (coordinator *Coordinator) RemoveProducerById(id uint8, reason Event) error {
	producer, err := coordinator.ExtractProducerById(id)
	if err != nil {
		return err
	}
	return producer.close(reason)
}

func (coordinator *Coordinator) RemoveResponseById(id interface{}) error {
	resp, err := coordinator.GetResponseByName(fmt.Sprintf("%d", id))
	if err != nil {
		return err
	}

	err = coordinator.removeById(fmt.Sprintf("%d", id), coordinator.responses)
	close(resp.code)
	close(resp.data)
	return err
}

func (coordinator *Coordinator) ProducersCount() int {
	return coordinator.countSyncMap(coordinator.producers)
}

// response
func newResponse(commandDescription string) *Response {
	res := &Response{}
	res.commandDescription = commandDescription
	res.code = make(chan Code, 1)
	res.data = make(chan interface{}, 1)
	return res
}

func (coordinator *Coordinator) NewResponseWitName(id string) *Response {
	coordinator.mutex.Lock()
	coordinator.counter++
	res := newResponse(id)
	res.correlationid = coordinator.counter
	coordinator.responses[id] = res
	coordinator.mutex.Unlock()
	return res
}

func (coordinator *Coordinator) NewResponse(commandId uint16, info ...string) *Response {
	description := lookUpCommand(commandId)
	if len(info) > 0 {
		description = fmt.Sprintf("%s, - %s", description, info[0])
	}

	coordinator.mutex.Lock()
	defer coordinator.mutex.Unlock()
	coordinator.counter++
	res := newResponse(description)
	res.correlationid = coordinator.counter
	coordinator.responses[strconv.Itoa(coordinator.counter)] = res
	return res
}

func (coordinator *Coordinator) GetResponseByName(id string) (*Response, error) {
	coordinator.mutex.Lock()
	defer coordinator.mutex.Unlock()
	if coordinator.responses[id] == nil {
		return nil, errors.New("Response #{id} not found ")
	}
	switch coordinator.responses[id].(type) {
	case *Response:
		return coordinator.responses[id].(*Response), nil
	}

	return nil, nil
}

func (coordinator *Coordinator) RemoveResponseByName(id string) error {
	coordinator.mutex.Lock()
	defer coordinator.mutex.Unlock()
	if coordinator.responses[id] == nil {
		return errors.New("Response #{id} not found ")
	}
	coordinator.responses[id] = nil
	delete(coordinator.responses, id)
	return nil
}

// Consumer functions
func (coordinator *Coordinator) NewConsumer(messagesHandler MessagesHandler,
	parameters *ConsumerOptions, cleanUp func()) *Consumer {
	coordinator.mutex.Lock()
	defer coordinator.mutex.Unlock()
	var lastId, _ = coordinator.getNextConsumerItem()
	var item = &Consumer{ID: lastId, options: parameters,
		response:             newResponse(lookUpCommand(commandSubscribe)),
		status:               open,
		mutex:                &sync.Mutex{},
		MessagesHandler:      messagesHandler,
		currentOffset:        -1, // currentOffset has to equal lastStoredOffset as the currentOffset 0 may otherwise be flushed to the server when the consumer is closed and auto commit is enabled
		lastStoredOffset:     -1, // because 0 is a valid value for the offset
		isPromotedAsActive:   true,
		lastAutoCommitStored: time.Now(),
		chunkForConsumer:     make(chan chunkInfo, 100),
		onClose:              cleanUp,
	}

	coordinator.consumers.Store(lastId, item)

	return item
}

func (coordinator *Coordinator) GetConsumerById(id interface{}) (*Consumer, error) {
	if consumer, exists := coordinator.consumers.Load(id); exists {
		return consumer.(*Consumer), nil
	}

	return nil, errors.New("item #{id} not found ")
}

func (coordinator *Coordinator) ExtractConsumerById(id interface{}) (*Consumer, error) {
	if consumer, exists := coordinator.consumers.LoadAndDelete(id); exists {
		return consumer.(*Consumer), nil
	}

	return nil, errors.New("item #{id} not found ")
}

func (coordinator *Coordinator) GetResponseById(id uint32) (*Response, error) {
	v, err := coordinator.getById(fmt.Sprintf("%d", id), coordinator.responses)
	if err != nil {
		return nil, err
	}
	return v.(*Response), err
}

func (coordinator *Coordinator) ConsumersCount() int {
	return coordinator.countSyncMap(coordinator.consumers)
}

func (coordinator *Coordinator) GetProducerById(id interface{}) (*Producer, error) {
	if producer, exists := coordinator.producers.Load(id); exists {
		return producer.(*Producer), nil
	}

	return nil, errors.New("item #{id} not found ")
}

func (coordinator *Coordinator) ExtractProducerById(id interface{}) (*Producer, error) {
	if producer, exists := coordinator.producers.LoadAndDelete(id); exists {
		return producer.(*Producer), nil
	}

	return nil, errors.New("item #{id} not found ")
}

// general functions
func (coordinator *Coordinator) getById(id interface{}, refmap map[interface{}]interface{}) (interface{}, error) {
	coordinator.mutex.Lock()
	defer coordinator.mutex.Unlock()
	if refmap[id] == nil {
		return nil, errors.New("item #{id} not found ")
	}
	return refmap[id], nil
}

func (coordinator *Coordinator) removeById(id interface{}, refmap map[interface{}]interface{}) error {
	coordinator.mutex.Lock()
	defer coordinator.mutex.Unlock()
	if refmap[id] == nil {
		return fmt.Errorf("remove failed, id %d not found", id)
	}
	refmap[id] = nil
	delete(refmap, id)
	return nil
}

func (coordinator *Coordinator) countSyncMap(refmap *sync.Map) int {
	count := 0
	refmap.Range(func(_, _ interface{}) bool {
		count++
		return true
	})

	return count
}

func (coordinator *Coordinator) getNextProducerItem() (uint8, error) {
	if coordinator.nextItemProducer >= ^uint8(0) {
		return coordinator.reuseFreeId(coordinator.producers)
	}
	res := coordinator.nextItemProducer
	coordinator.nextItemProducer++
	return res, nil
}

func (coordinator *Coordinator) getNextConsumerItem() (uint8, error) {
	if coordinator.nextItemConsumer >= ^uint8(0) {
		return coordinator.reuseFreeId(coordinator.consumers)
	}
	res := coordinator.nextItemConsumer
	coordinator.nextItemConsumer++
	return res, nil
}

func (coordinator *Coordinator) reuseFreeId(refMap *sync.Map) (byte, error) {
	maxValue := int(^uint8(0))
	var result byte
	for i := 0; i < maxValue; i++ {
		if _, exists := refMap.Load(byte(i)); !exists {
			return byte(i), nil
		}
		result++
	}
	if result >= ^uint8(0) {
		return 0, errors.New("No more items available")
	}
	return result, nil
}

func (coordinator *Coordinator) Producers() *sync.Map {
	return coordinator.producers
}

func (coordinator *Coordinator) Close() {
	coordinator.producers.Range(func(_, producer interface{}) bool {
		_ = producer.(*Producer).Close()

		return true
	})

	coordinator.consumers.Range(func(_, consumer interface{}) bool {
		_ = consumer.(*Consumer).Close()

		return true
	})
}
