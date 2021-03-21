package stream

import (
	"github.com/pkg/errors"
	"strconv"
	"sync"
)

type Producers struct {
	items map[byte]*Producer
	mutex *sync.Mutex
}

type Consumers struct {
	items map[byte]*Consumer
	mutex *sync.Mutex
}

type Responses struct {
	counter int
	items   map[string]*Response
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

func NewProducers() *Producers {
	return &Producers{mutex: &sync.Mutex{},
		items: make(map[byte]*Producer)}
}

func (c *Producers) New(linkedClient *Client) *Producer {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	var lastId = uint8(len(c.items))
	var producer = &Producer{ID: lastId, response:
	&Response{code: make(chan Code)}}
	producer.LikedClient = linkedClient
	c.items[lastId] = producer
	return producer
}

func (c *Producers) CloseAll() error {
	for _, i2 := range c.items {
		err := i2.Close()
		if err != nil {
			return err
		}

	}
	return nil
}

func (c *Producers) GetById(id uint8) (*Producer, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.items[id] == nil {
		return nil, errors.New("Producer #{id} not found ")
	}
	return c.items[id], nil
}

func (c *Producers) RemoveById(id byte) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.items[id] == nil {
		return errors.New("Producer #{id} not found ")
	}
	delete(c.items, id)
	return nil
}

func (c *Producers) Count() int {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return len(c.items)
}

func NewResponses() *Responses {
	return &Responses{mutex: &sync.Mutex{},
		items: make(map[string]*Response)}
}

func NewResponse() *Response {
	res := &Response{}
	res.code = make(chan Code, 0)
	res.data = make(chan interface{}, 0)
	return res
}

func (s *Responses) NewWitName(value string) *Response {
	s.mutex.Lock()
	s.counter++
	res := NewResponse()
	res.subId = s.counter
	s.items[value] = res
	s.mutex.Unlock()
	return res
}

func (s *Responses) New() *Response {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.counter++
	res := NewResponse()
	res.subId = s.counter
	s.items[strconv.Itoa(s.counter)] = res
	return res
}

func (s *Responses) GetById(id uint32) (*Response, error) {
	sa := strconv.Itoa(int(id))
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if s.items[sa] == nil {
		return nil, errors.New("Response #{id} not found ")
	}
	return s.items[sa], nil
}

func (s *Responses) GetByName(id string) (*Response, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if s.items[id] == nil {
		return nil, errors.New("Response #{id} not found ")
	}
	return s.items[id], nil
}

func (s *Responses) RemoveById(id int) error {
	sa := strconv.Itoa(id)
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if s.items[sa] == nil {
		return errors.New("Response #{id} not found ")
	}
	delete(s.items, sa)
	return nil
}

func (s *Responses) RemoveByName(id string) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if s.items[id] == nil {
		return errors.New("Response #{id} not found ")
	}
	delete(s.items, id)
	return nil
}

func (s *Responses) Count() int {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return len(s.items)
}

func NewConsumers() *Consumers {
	return &Consumers{mutex: &sync.Mutex{},
		items: make(map[byte]*Consumer)}
}

func (c *Consumers) New(linkedClient *Client) *Consumer {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	var lastId = uint8(len(c.items))
	var item = &Consumer{ID: lastId, response: NewResponse()}
	item.LikedClient = linkedClient
	c.items[lastId] = item
	return item
}

func (c *Consumers) GetById(id uint8) (*Consumer, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.items[id] == nil {
		return nil, errors.New("Consumer #{id} not found ")
	}
	return c.items[id], nil
}


func (c *Consumers) Count() int {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return len(c.items)
}


func (c *Consumers) RemoveById(id byte) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.items[id] == nil {
		return errors.New("Consumer #{id} not found ")
	}
	delete(c.items, id)
	return nil
}

func (c *Consumers) UnsubscribeAll() error {
	for _, i2 := range c.items {
		err := i2.UnSubscribe()
		if err != nil {
			return err
		}

	}
	return nil
}