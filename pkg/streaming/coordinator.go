package streaming

import (
	"fmt"
	"github.com/pkg/errors"
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
	correlationid int
}

func NewCoordinator() *Coordinator {
	return &Coordinator{mutex: &sync.Mutex{},
		producers: make(map[interface{}]interface{}),
		consumers: make(map[interface{}]interface{}),
		responses: make(map[interface{}]interface{})}
}

// producers
func (items *Coordinator) NewProducer(parameters *ProducerCreator) *Producer {
	items.mutex.Lock()
	defer items.mutex.Unlock()
	var lastId = uint8(len(items.producers))
	var producer = &Producer{ID: lastId,
		parameters: parameters}
	items.producers[lastId] = producer
	return producer
}

func (items *Coordinator) RemoveConsumerById(id interface{}) error {
	return items.removeById(id, items.consumers)
}
func (items *Coordinator) RemoveProducerById(id interface{}) error {
	return items.removeById(id, items.producers)
}

func (items *Coordinator) RemoveResponseById(id interface{}) error {
	return items.removeById(fmt.Sprintf("%d", id), items.responses)
}


func (items *Coordinator) ProducersCount() int {
	return items.count(items.producers)
}



/// response
func NewResponse() *Response {
	res := &Response{}
	res.code = make(chan Code, 0)
	res.data = make(chan interface{}, 0)
	return res
}

func (items *Coordinator) NewResponseWitName(id string) *Response {
	items.mutex.Lock()
	items.counter++
	res := NewResponse()
	res.correlationid = items.counter
	items.responses[id] = res
	items.mutex.Unlock()
	return res
}

func (items *Coordinator) NewResponse() *Response {
	items.mutex.Lock()
	defer items.mutex.Unlock()
	items.counter++
	res := NewResponse()
	res.correlationid = items.counter
	items.responses[strconv.Itoa(items.counter)] = res
	return res
}

func (items *Coordinator) GetResponseByName(id string) (*Response, error) {
	items.mutex.Lock()
	defer items.mutex.Unlock()
	if items.responses[id] == nil {
		return nil, errors.New("Response #{id} not found ")
	}
	switch items.responses[id].(type) {
	case *Response:
		return items.responses[id].(*Response), nil
	}

	return nil, nil
}

func (items *Coordinator) RemoveResponseByName(id string) error {
	items.mutex.Lock()
	defer items.mutex.Unlock()
	if items.responses[id] == nil {
		return errors.New("Response #{id} not found ")
	}
	delete(items.responses, id)
	return nil
}

func (items *Coordinator) ResponsesCount() int {
	return items.count(items.responses)
}


/// Consumer functions
func (items *Coordinator) NewConsumer(parameters *ConsumerCreator) *Consumer {
	items.mutex.Lock()
	defer items.mutex.Unlock()
	var lastId = uint8(len(items.consumers))
	var item = &Consumer{ID: lastId, parameters: parameters,
		response: NewResponse()}
	items.consumers[lastId] = item
	return item
}

func (items *Coordinator) GetConsumerById(id interface{}) (*Consumer, error) {
	v, err := items.getById(id, items.consumers)
	if err != nil {
		return nil, err
	}
	return v.(*Consumer), err
}

func (items *Coordinator) GetResponseById(id uint32) (*Response, error) {
	v, err := items.getById(fmt.Sprintf("%d", id), items.responses)
	if err != nil {
		return nil, err
	}
	return v.(*Response), err
}

func (items *Coordinator) ConsumersCount() int {
	return items.count(items.consumers)
}



// general functions

func (items *Coordinator) getById(id interface{}, refmap map[interface{}]interface{}) (interface{}, error) {
	items.mutex.Lock()
	defer items.mutex.Unlock()
	if refmap[id] == nil {
		return nil, errors.New("Item #{id} not found ")
	}
	return refmap[id], nil
}

func (items *Coordinator) removeById(id interface{}, refmap map[interface{}]interface{}) error {
	items.mutex.Lock()
	defer items.mutex.Unlock()
	if refmap[id] == nil {
		return errors.New("item #{id} not found ")
	}
	delete(refmap, id)
	return nil
}

func (items *Coordinator) count(refmap map[interface{}]interface{}) int {
	items.mutex.Lock()
	defer items.mutex.Unlock()
	return len(refmap)
}
