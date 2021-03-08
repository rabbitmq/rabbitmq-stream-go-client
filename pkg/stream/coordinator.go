package stream

import (
	"fmt"
	"strconv"
	"sync"
)

var producers *Producers
var responses *Responses

type Producers struct {
	items       map[byte]*Producer
	mutex       *sync.Mutex
	LikedClient *Client
}

type Responses struct {
	items map[string]*Response
	mutex *sync.Mutex
}

type Response struct {
	isDone     chan bool
	dataString chan []string
	dataBytes  chan []byte
	subId      int32
}

func InitCoordinators() {
	producers = &Producers{}
	producers.mutex = &sync.Mutex{}
	producers.items = make(map[byte]*Producer)

	responses = &Responses{}
	responses.mutex = &sync.Mutex{}
	responses.items = make(map[string]*Response)
}

func GetProducers() *Producers {
	return producers
}

func (c *Producers) registerNewProducer() (*Producer, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	var lastId = byte(len(c.items))
	var producer = &Producer{ProducerID: lastId, PublishConfirm: &Response{isDone: make(chan bool)}}
	c.items[lastId] = producer
	return producer, nil
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

func (c Producers) GetProducerById(id byte) *Producer {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.items[id] == nil {
		fmt.Printf("errrrr Producer nill  %d \n", id)
		return nil
	}
	return c.items[id]
}

// TODO: remove publisher

func GetResponses() *Responses {
	return responses
}

func (s Responses) addResponderWitName(value string) *Response {

	s.mutex.Lock()
	var lastId int32
	lastId = int32(len(s.items))
	res := &Response{}
	res.isDone = make(chan bool, 1)
	res.dataString = make(chan []string, 0)
	res.dataBytes = make(chan []byte, 0)
	res.subId = lastId
	s.items[value] = res
	s.mutex.Unlock()
	return res
}

func (s Responses) addResponder() *Response {
	s.mutex.Lock()
	var lastId int32
	lastId = int32(len(s.items))
	res := &Response{}
	res.isDone = make(chan bool, 1)
	res.dataString = make(chan []string, 0)
	res.dataBytes = make(chan []byte, 0)

	res.subId = lastId
	s.items[strconv.Itoa(int(lastId))] = res
	s.mutex.Unlock()
	return res
}

func (s Responses) GetResponderById(id uint32) *Response {
	sa := strconv.Itoa(int(id))
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if s.items[sa] == nil {
		fmt.Printf("errrrr Response nill %s \n", sa)
		return nil
	}
	return s.items[sa]
}

func (s Responses) GetResponderByName(id string) *Response {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.items[id]
}
