package stream

import (
	"fmt"
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
	items map[int32]*Response
	mutex *sync.Mutex
}

type Response struct {
	isDone     chan bool
	dataString chan []string
	dataBytes chan []byte
	subId      int32
}

func InitCoordinators() {
	producers = &Producers{}
	producers.mutex = &sync.Mutex{}
	producers.items = make(map[byte]*Producer)

	responses = &Responses{}
	responses.mutex = &sync.Mutex{}
	responses.items = make(map[int32]*Response)
}

func GetProducers() *Producers {
	return producers
}

func (c *Producers) registerNewProducer() (*Producer, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	var lastId = byte(len(c.items))
	var producer = &Producer{ProducerID: lastId}
	c.items[lastId] = producer
	return producer, nil
}

// TODO: remove publisher

func GetResponses() *Responses {
	return responses
}

func (s Responses) addResponder() *Response {

	s.mutex.Lock()
	var lastId int32
	lastId = int32(len(s.items))
	res := &Response{}
	res.isDone = make(chan bool, 1)
	res.dataString = make(chan []string, 1)
	res.subId = lastId
	s.items[lastId] = res
	s.mutex.Unlock()
	return res
}

func (s Response) wait() int32 {
	<-s.isDone
	fmt.Printf("Subscriber is done: %d \n", s.subId)
	return s.subId
}
