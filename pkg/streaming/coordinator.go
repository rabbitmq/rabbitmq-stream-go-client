package streaming

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"strconv"
	"sync"
)

type Coordinator struct {
	counter   int
	producers map[interface{}]interface{}
	consumers map[interface{}]interface{}
	responses map[interface{}]interface{}
	mutex     *sync.Mutex
}

type Code struct {
	id uint16
}

type Response struct {
	code          chan Code
	data          chan interface{}
	messages      chan []*amqp.Message
	correlationid int
}

func NewCoordinator() *Coordinator {
	return &Coordinator{mutex: &sync.Mutex{},
		producers: make(map[interface{}]interface{}),
		consumers: make(map[interface{}]interface{}),
		responses: make(map[interface{}]interface{})}
}

// producers
func (coordinator *Coordinator) NewProducer(parameters *ProducerOptions) (*Producer, error) {
	coordinator.mutex.Lock()
	defer coordinator.mutex.Unlock()
	var lastId, err = coordinator.getNextFreeId(coordinator.producers)
	if err != nil {
		return nil, err
	}
	var producer = &Producer{ID: lastId,
		parameters: parameters}
	coordinator.producers[lastId] = producer
	return producer, err
}

func (coordinator *Coordinator) RemoveConsumerById(id interface{}) error {
	return coordinator.removeById(id, coordinator.consumers)
}
func (coordinator *Coordinator) RemoveProducerById(id uint8) error {
	return coordinator.removeById(id, coordinator.producers)
}

func (coordinator *Coordinator) RemoveResponseById(id interface{}) error {
	return coordinator.removeById(fmt.Sprintf("%d", id), coordinator.responses)
}

func (coordinator *Coordinator) ProducersCount() int {
	return coordinator.count(coordinator.producers)
}

/// response
func NewResponse() *Response {
	res := &Response{}
	res.code = make(chan Code)
	res.data = make(chan interface{})
	res.messages = make(chan []*amqp.Message, 100)
	return res
}

func (coordinator *Coordinator) NewResponseWitName(id string) *Response {
	coordinator.mutex.Lock()
	coordinator.counter++
	res := NewResponse()
	res.correlationid = coordinator.counter
	coordinator.responses[id] = res
	coordinator.mutex.Unlock()
	return res
}

func (coordinator *Coordinator) NewResponse() *Response {
	coordinator.mutex.Lock()
	defer coordinator.mutex.Unlock()
	coordinator.counter++
	res := NewResponse()
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

func (coordinator *Coordinator) ResponsesCount() int {
	return coordinator.count(coordinator.responses)
}

/// Consumer functions
func (coordinator *Coordinator) NewConsumer(parameters *ConsumerCreator) *Consumer {
	coordinator.mutex.Lock()
	defer coordinator.mutex.Unlock()
	var lastId, _ = coordinator.getNextFreeId(coordinator.consumers)
	var item = &Consumer{ID: lastId, parameters: parameters,
		response: NewResponse(), mutex: &sync.RWMutex{}}
	coordinator.consumers[lastId] = item
	return item
}

func (coordinator *Coordinator) GetConsumerById(id interface{}) (*Consumer, error) {
	v, err := coordinator.getById(id, coordinator.consumers)
	if err != nil {
		return nil, err
	}
	return v.(*Consumer), err
}

func (coordinator *Coordinator) GetResponseById(id uint32) (*Response, error) {
	v, err := coordinator.getById(fmt.Sprintf("%d", id), coordinator.responses)
	if err != nil {
		return nil, err
	}
	return v.(*Response), err
}

func (coordinator *Coordinator) ConsumersCount() int {
	return coordinator.count(coordinator.consumers)
}

// general functions

func (coordinator *Coordinator) getById(id interface{}, refmap map[interface{}]interface{}) (interface{}, error) {
	coordinator.mutex.Lock()
	defer coordinator.mutex.Unlock()
	if refmap[id] == nil {
		return nil, errors.New("Item #{id} not found ")
	}
	return refmap[id], nil
}

func (coordinator *Coordinator) removeById(id interface{}, refmap map[interface{}]interface{}) error {
	coordinator.mutex.Lock()
	defer coordinator.mutex.Unlock()
	if refmap[id] == nil {
		return errors.New("item #{id} not found ")
	}
	refmap[id] = nil
	delete(refmap, id)
	return nil
}

func (coordinator *Coordinator) count(refmap map[interface{}]interface{}) int {
	coordinator.mutex.Lock()
	defer coordinator.mutex.Unlock()
	return len(refmap)
}

func (coordinator *Coordinator) getNextFreeId(refmap map[interface{}]interface{}) (byte, error) {
	maxValue := int(^uint8(0))
	var result byte
	for i := 0; i < maxValue; i++ {
		if refmap[byte(i)] == nil {
			return byte(i), nil
		}
		result++
	}
	if result >= ^uint8(0) {
		return 0, errors.New("No more items available")
		// TODO HANDLE THE ERROR
	}
	return result, nil
}
