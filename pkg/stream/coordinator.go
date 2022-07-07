package stream

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"strconv"
	"sync"
)

type Entity interface {
	GetID() uint8
	getDataChannel() chan ServerResponse
}

type Coordinator struct {
	entity Entity

	responses map[interface{}]interface{}

	counter          int
	consumers        map[interface{}]interface{}
	nextItemConsumer uint8
	mutex            *sync.Mutex
}

type Code struct {
	id uint16
}

type offsetMessages struct {
	messages []*amqp.Message
	offset   int64
}

type Response struct {
	code               chan Code
	data               chan interface{}
	offsetMessages     chan offsetMessages
	commandDescription string
	correlationid      int
}

func NewCoordinator() *Coordinator {
	return &Coordinator{mutex: &sync.Mutex{},
		consumers: make(map[interface{}]interface{}),
		responses: make(map[interface{}]interface{})}
}

func (coordinator *Coordinator) RemoveConsumerById(id interface{}, reason Event) error {
	consumer, err := coordinator.GetConsumerById(id)
	if err != nil {
		return err
	}
	reason.StreamName = consumer.GetStreamName()
	reason.Name = consumer.GetName()

	if consumer.closeHandler != nil {
		consumer.closeHandler <- reason
		close(consumer.closeHandler)
	}

	return coordinator.removeById(id, coordinator.consumers)
}

func (coordinator *Coordinator) RemoveResponseById(id interface{}) error {
	resp, err := coordinator.GetResponseByName(fmt.Sprintf("%d", id))
	if err != nil {
		return err
	}

	err = coordinator.removeById(fmt.Sprintf("%d", id), coordinator.responses)
	close(resp.code)
	close(resp.data)
	close(resp.offsetMessages)
	return err
}

/// response
func newResponse(commandDescription string) *Response {
	res := &Response{}
	res.commandDescription = commandDescription
	res.code = make(chan Code, 1)
	res.data = make(chan interface{})
	res.offsetMessages = make(chan offsetMessages, 100)
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

/// Consumer functions
func (coordinator *Coordinator) NewConsumer(messagesHandler MessagesHandler,
	parameters *ConsumerOptions) *Consumer {
	coordinator.mutex.Lock()
	defer coordinator.mutex.Unlock()
	var lastId, _ = coordinator.getNextConsumerItem()
	var item = &Consumer{ID: lastId, options: parameters,
		response:        newResponse(lookUpCommand(commandSubscribe)),
		status:          open,
		mutex:           &sync.Mutex{},
		MessagesHandler: messagesHandler,
	}

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

func (coordinator *Coordinator) count(refmap map[interface{}]interface{}) int {
	coordinator.mutex.Lock()
	defer coordinator.mutex.Unlock()
	return len(refmap)
}

func (coordinator *Coordinator) getNextConsumerItem() (uint8, error) {
	if coordinator.nextItemConsumer >= ^uint8(0) {
		return coordinator.reuseFreeId(coordinator.consumers)
	}
	res := coordinator.nextItemConsumer
	coordinator.nextItemConsumer++
	return res, nil
}

func (coordinator *Coordinator) reuseFreeId(refMap map[interface{}]interface{}) (byte, error) {
	maxValue := int(^uint8(0))
	var result byte
	for i := 0; i < maxValue; i++ {
		if refMap[byte(i)] == nil {
			return byte(i), nil
		}
		result++
	}
	if result >= ^uint8(0) {
		return 0, errors.New("No more items available")
	}
	return result, nil
}
