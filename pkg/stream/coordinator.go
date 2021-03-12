package stream

import (
	"github.com/pkg/errors"
	"strconv"
	"sync"
)

type Producers struct {
	items       map[byte]*Producer
	mutex       *sync.Mutex
	LikedClient *Client
}

type Responses struct {
	items map[string]*Response
	mutex *sync.Mutex
}

func NewProducers() *Producers {
	return &Producers{mutex: &sync.Mutex{},
		items: make(map[byte]*Producer)}
}

func NewResponses() *Responses {
	return &Responses{mutex: &sync.Mutex{},
		items: make(map[string]*Response)}
}

func (c *Producers) New() *Producer {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	var lastId = uint8(len(c.items))
	var producer = &Producer{ProducerID: lastId, PublishConfirm:
	&Response{isDone: make(chan bool)}}
	c.items[lastId] = producer
	return producer
}

func (c *Producers) CloseAllProducers() error {
	for _, i2 := range c.items {
		err := i2.Close()
		if err != nil {
			return err
		}

	}
	return nil
}

func (c Producers) GetById(id uint8) (*Producer, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.items[id] == nil {
		return nil, errors.New("Producer not found #{id}")
	}
	return c.items[id], nil
}

func (c Producers) RemoveById(id byte) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.items[id] == nil {
		return errors.New("Producer not found #{id}")
	}
	delete(c.items, id)

	return nil
}

func (c Producers) Count() int {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return len(c.items)
}

func newResponse() *Response {
	res := &Response{}
	res.isDone = make(chan bool, 1)
	res.dataString = make(chan []string, 0)
	res.dataBytes = make(chan []byte, 0)
	return res
}
func (s Responses) NewWitName(value string) *Response {
	s.mutex.Lock()
	var lastId int
	lastId = len(s.items)
	res := newResponse()
	res.subId = lastId
	s.items[value] = res
	s.mutex.Unlock()
	return res
}

func (s Responses) New() *Response {
	s.mutex.Lock()
	var lastId int
	lastId = len(s.items)
	res := newResponse()
	res.subId = lastId
	s.items[strconv.Itoa(int(lastId))] = res
	s.mutex.Unlock()
	return res
}

func (s Responses) GetById(id uint32) (*Response, error) {
	sa := strconv.Itoa(int(id))
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if s.items[sa] == nil {
		return nil, errors.New("Response not found #{id}")
	}
	return s.items[sa], nil
}

func (s Responses) GetByName(id string) (*Response, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if s.items[id] == nil {
		return nil, errors.New("Response not found #{id}")
	}
	return s.items[id], nil
}

func (s Responses) RemoveById(id int) error {
	sa := strconv.Itoa(id)
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if s.items[sa] == nil {
		return errors.New("Response not found #{id}")
	}
	delete(s.items, sa)

	return nil
}

func (s Responses) Count() int {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return len(s.items)
}
