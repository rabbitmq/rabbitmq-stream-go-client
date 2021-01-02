package stream

import "sync"

var producersCoordinator *ProducersCoordinator

type ProducersCoordinator struct {
	producers   map[byte]*Producer
	mutex       *sync.Mutex
	LikedClient *Client
}

func InitProducersCoordinator() {
	producersCoordinator = &ProducersCoordinator{}
	producersCoordinator.mutex = &sync.Mutex{}
	producersCoordinator.producers = make(map[byte]*Producer)
}

func GetProducersCoordinator() *ProducersCoordinator {
	return producersCoordinator
}

func (c *ProducersCoordinator) registerNewProducer() (*Producer, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	var lastId byte
	for _, key := range c.producers {
		lastId = key.ProducerID + 1
	}
	var producer = &Producer{ProducerID: lastId}
	c.producers[lastId] = producer

	return producer, nil
}
