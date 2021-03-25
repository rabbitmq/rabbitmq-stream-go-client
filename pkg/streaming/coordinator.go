package streaming

import (
	"fmt"
	"github.com/pkg/errors"
	"strconv"
	"sync"
)

type Items struct {
	counter int
	items   map[interface{}]interface{}
	mutex   *sync.Mutex
}

type Code struct {
	id          uint16
	description string
}

type Response struct {
	code  chan Code
	data  chan interface{}
	subId int
}

func NewItems() *Items {
	return &Items{mutex: &sync.Mutex{},
		items: make(map[interface{}]interface{})}
}

func (items *Items) NewProducer(parameters *ProducerCreator) *Producer {
	items.mutex.Lock()
	defer items.mutex.Unlock()
	var lastId = uint8(len(items.items))
	var producer = &Producer{ID: lastId, response:
	&Response{code: make(chan Code)}}
	producer.parameters = parameters
	items.items[lastId] = producer
	return producer
}

func (items *Items) RemoveResponseById(id interface{}) error {
	return items.RemoveById(fmt.Sprintf("%d", id))

}
func (items *Items) RemoveById(id interface{}) error {
	items.mutex.Lock()
	defer items.mutex.Unlock()
	if items.items[id] == nil {
		return errors.New("item #{id} not found ")
	}
	delete(items.items, id)
	return nil
}

func (items *Items) Count() int {
	items.mutex.Lock()
	defer items.mutex.Unlock()
	return len(items.items)
}

func NewResponses() *Items {
	return &Items{mutex: &sync.Mutex{},
		items: make(map[interface{}]interface{})}
}

func NewResponse() *Response {
	res := &Response{}
	res.code = make(chan Code, 0)
	res.data = make(chan interface{}, 0)
	return res
}

func (items *Items) NewResponseWitName(id string) *Response {
	items.mutex.Lock()
	items.counter++
	res := NewResponse()
	res.subId = items.counter
	items.items[id] = res
	items.mutex.Unlock()
	return res
}

func (items *Items) NewResponse() *Response {
	items.mutex.Lock()
	defer items.mutex.Unlock()
	items.counter++
	res := NewResponse()
	res.subId = items.counter
	items.items[strconv.Itoa(items.counter)] = res
	return res
}

func (items *Items) GetResponseByName(id string) (*Response, error) {
	items.mutex.Lock()
	defer items.mutex.Unlock()
	if items.items[id] == nil {
		return nil, errors.New("Response #{id} not found ")
	}
	switch items.items[id].(type) {
	case *Response:
		return items.items[id].(*Response), nil
	}

	return nil, nil
}

func (items *Items) RemoveResponseByName(id string) error {
	items.mutex.Lock()
	defer items.mutex.Unlock()
	if items.items[id] == nil {
		return errors.New("Response #{id} not found ")
	}
	delete(items.items, id)
	return nil
}

func (items *Items) NewConsumer(parameters *ConsumerCreator) *Consumer {
	items.mutex.Lock()
	defer items.mutex.Unlock()
	var lastId = uint8(len(items.items))
	var item = &Consumer{ID: lastId, parameters: parameters,
		response: NewResponse()}
	items.items[lastId] = item
	return item
}

func (items *Items) GetConsumerById(id interface{}) (*Consumer, error) {
	v, err := items.getById(id)
	if err != nil {
		return nil, err
	}
	return v.(*Consumer), err
}

func (items *Items) GetResponseById(id uint32) (*Response, error) {
	v, err := items.getById(fmt.Sprintf("%d", id))
	if err != nil {
		return nil, err
	}
	return v.(*Response), err
}

func (items *Items) getById(id interface{}) (interface{}, error) {
	items.mutex.Lock()
	defer items.mutex.Unlock()
	if items.items[id] == nil {
		return nil, errors.New("Item #{id} not found ")
	}
	return items.items[id], nil
}
