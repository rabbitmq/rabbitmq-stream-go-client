package streaming

import (
	"sync"
)

type Environment struct {
	clientLocator        *Client
	producers            *producers
	PublishErrorListener PublishErrorListener
}

func NewEnvironment(options *EnvironmentOptions) (*Environment, error) {
	client := NewClient()

	if options == nil {
		options = NewEnvironmentOptions()
	}

	client.broker = options.LocatorBroker

	return &Environment{
		clientLocator: client,
		producers: newProducers(options.maxProducersPerClient,
			options.PublishErrorListener),
		PublishErrorListener: options.PublishErrorListener,
	}, client.connect()
}

func (env *Environment) DeclareStream(streamName string, options *StreamOptions) error {
	return env.clientLocator.DeclareStream(streamName, options)
}

func (env *Environment) DeleteStream(streamName string) error {
	return env.clientLocator.DeleteStream(streamName)
}

func (env *Environment) NewProducer(streamName string, producerOptions *ProducerOptions) (*Producer, error) {
	if producerOptions == nil {
		producerOptions = NewProducerOptions()
	}
	return env.producers.NewProducer(env.clientLocator, streamName, producerOptions)
}

func (env *Environment) Close() error {
	_ = env.producers.close()
	return env.clientLocator.Close()
}

type EnvironmentOptions struct {
	LocatorBroker         Broker
	maxProducersPerClient int
	PublishErrorListener  PublishErrorListener
}

func NewEnvironmentOptions() *EnvironmentOptions {
	return &EnvironmentOptions{
		maxProducersPerClient: 3,
		LocatorBroker:         newBrokerDefault(),
	}
}

func (envOptions *EnvironmentOptions) MaxProducersPerClient(value int) *EnvironmentOptions {
	envOptions.maxProducersPerClient = value
	return envOptions

}

func (envOptions *EnvironmentOptions) Uri(uri string) *EnvironmentOptions {
	envOptions.LocatorBroker.Uri = uri
	return envOptions
}

func (envOptions *EnvironmentOptions) UserName(user string) *EnvironmentOptions {
	envOptions.LocatorBroker.User = user
	return envOptions
}

func (envOptions *EnvironmentOptions) Password(password string) *EnvironmentOptions {
	envOptions.LocatorBroker.Password = password
	return envOptions
}

func (envOptions *EnvironmentOptions) Host(host string) *EnvironmentOptions {
	envOptions.LocatorBroker.Host = host
	return envOptions
}

func (envOptions *EnvironmentOptions) Port(port int) *EnvironmentOptions {
	envOptions.LocatorBroker.Port = port
	return envOptions
}

func (envOptions *EnvironmentOptions) OnPublishError(publishErrorListener PublishErrorListener) *EnvironmentOptions {
	envOptions.PublishErrorListener = publishErrorListener
	return envOptions
}

type producersCoordinator struct {
	mutex             *sync.Mutex
	clientsPerContext map[int]*Client
	maxItemsForClient int
	nextId            int
}

func (cc *producersCoordinator) isFull(clientsPerContextId int) bool {
	return cc.clientsPerContext[clientsPerContextId].coordinator.
		ProducersCount() >= cc.maxItemsForClient
}

func (cc *producersCoordinator) maybeCleanClients() {
	cc.mutex.Lock()
	defer cc.mutex.Unlock()
	for i, client := range cc.clientsPerContext {
		if !client.socket.isOpen() {
			delete(cc.clientsPerContext, i)
		}
	}
}

func (cc *producersCoordinator) maybeCleanProducers(streamName string) {
	cc.mutex.Lock()
	defer cc.mutex.Unlock()
	for _, client := range cc.clientsPerContext {
		for pidx, producer := range client.coordinator.producers {
			if producer.(*Producer).parameters.streamName == streamName {
				err := client.coordinator.RemoveProducerById(pidx.(uint8))
				if err != nil {
					return
				}
			}
		}
		if client.coordinator.ProducersCount() == 0 {
			err := client.Close()
			if err != nil {
				return
			}
		}
	}

	for i, client := range cc.clientsPerContext {
		if !client.socket.isOpen() {
			delete(cc.clientsPerContext, i)
		}
	}

}

func (cc *producersCoordinator) newProducer(leader *Broker, streamName string, listener PublishErrorListener) (*Producer, error) {
	cc.mutex.Lock()
	defer cc.mutex.Unlock()
	var clientResult *Client
	for i, client := range cc.clientsPerContext {
		if !cc.isFull(i) {
			clientResult = client
			break
		}
	}

	if clientResult == nil {
		clientResult = NewClient()
		clientResult.broker = *leader
		clientResult.PublishErrorListener = listener
		clientResult.metadataListener = func(ch <-chan string) {
			streamName := <-ch
			cc.maybeCleanProducers(streamName)
		}
		err := clientResult.connect()
		if err != nil {
			return nil, err
		}
		cc.nextId++
		cc.clientsPerContext[cc.nextId] = clientResult
	}

	publisher, err := clientResult.DeclarePublisher(streamName)

	if err != nil {
		return nil, err
	}

	return publisher, nil
}

func (cc *producersCoordinator) close() error {
	for _, client := range cc.clientsPerContext {
		err := client.Close()
		if err != nil {
			WARN("Error during close the client, %s", err)
		}
	}
	return nil
}

func (cc *producersCoordinator) getClientsPerContext() map[int]*Client {
	cc.mutex.Lock()
	defer cc.mutex.Unlock()
	return cc.clientsPerContext
}

type producers struct {
	mutex                *sync.Mutex
	clientCoordinator    map[string]*producersCoordinator
	maxItemsForClient    int
	PublishErrorListener PublishErrorListener
}

func newProducers(maxItemsForClient int, publishErrorListener PublishErrorListener) *producers {
	producers := &producers{
		mutex:                &sync.Mutex{},
		clientCoordinator:    map[string]*producersCoordinator{},
		maxItemsForClient:    maxItemsForClient,
		PublishErrorListener: publishErrorListener,
	}
	return producers
}

func (ps *producers) NewProducer(clientLocator *Client, streamName string, producerOptions *ProducerOptions) (*Producer, error) {
	ps.mutex.Lock()
	defer ps.mutex.Unlock()
	leader, err := clientLocator.BrokerLeader(streamName)
	if err != nil {
		return nil, err
	}
	if ps.clientCoordinator[leader.hostPort()] == nil {
		ps.clientCoordinator[leader.hostPort()] = &producersCoordinator{
			clientsPerContext: map[int]*Client{},
			mutex:             &sync.Mutex{},
			maxItemsForClient: ps.maxItemsForClient,
			nextId:            0,
		}
	}
	leader.cloneFrom(clientLocator.broker)

	producer, err := ps.clientCoordinator[leader.hostPort()].newProducer(leader, streamName, ps.PublishErrorListener)
	if err != nil {
		return nil, err
	}
	producer.onConsumerClosed = func(ch <-chan uint8) {
		for _, coordinator := range ps.clientCoordinator {
			coordinator.maybeCleanClients()
		}
	}
	producer.publishConfirm = producerOptions.publishConfirm

	return producer, err
}

func (ps *producers) close() error {
	ps.mutex.Lock()
	defer ps.mutex.Unlock()
	for _, coordinator := range ps.clientCoordinator {
		_ = coordinator.close()
	}
	return nil
}

func (ps *producers) getCoordinators() map[string]*producersCoordinator {
	ps.mutex.Lock()
	defer ps.mutex.Unlock()
	return ps.clientCoordinator

}
