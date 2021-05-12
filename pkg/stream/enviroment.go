package stream

import (
	"net/url"
	"sync"
)

type onClose func(ch <-chan uint8)

type Environment struct {
	clientLocator        *Client
	producers            *producersEnvironment
	consumers            *consumersEnvironment
	PublishErrorListener PublishErrorListener
}

func NewEnvironment(options *EnvironmentOptions) (*Environment, error) {
	client := NewClient()

	if options == nil {
		options = NewEnvironmentOptions()
	}
	if options.MaxConsumersPerClient == 0 {
		options.MaxConsumersPerClient = 3
	}

	if options.MaxProducersPerClient == 0 {
		options.MaxProducersPerClient = 3
	}

	if options.ConnectionParameters.Uri != "" {
		u, err := url.Parse(options.ConnectionParameters.Uri)
		if err != nil {
			return nil, err
		}
		options.ConnectionParameters.User = u.User.Username()
		options.ConnectionParameters.Password, _ = u.User.Password()
		//options.ConnectionParameters.Vhost = u.Path
	}

	client.broker = options.ConnectionParameters

	return &Environment{
		clientLocator: client,
		producers: newProducers(options.MaxProducersPerClient,
			options.PublishErrorListener),
		consumers:            newConsumerEnvironment(options.MaxConsumersPerClient),
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

func (env *Environment) NewConsumer(streamName string, messagesHandler MessagesHandler, options *ConsumerOptions) (*Consumer, error) {
	if options == nil {
		options = NewConsumerOptions()
	}
	return env.consumers.NewSubscriber(env.clientLocator, streamName, messagesHandler, options)
}

func (env *Environment) Close() error {
	_ = env.producers.close()
	_ = env.consumers.close()
	return env.clientLocator.Close()
}

type EnvironmentOptions struct {
	ConnectionParameters  Broker
	MaxProducersPerClient int
	MaxConsumersPerClient int
	PublishErrorListener  PublishErrorListener
}

func NewEnvironmentOptions() *EnvironmentOptions {
	return &EnvironmentOptions{
		MaxProducersPerClient: 3,
		MaxConsumersPerClient: 3,
		ConnectionParameters:  newBrokerDefault(),
	}
}

func (envOptions *EnvironmentOptions) SetMaxProducersPerClient(maxProducersPerClient int) *EnvironmentOptions {
	envOptions.MaxProducersPerClient = maxProducersPerClient
	return envOptions

}

func (envOptions *EnvironmentOptions) SetMaxConsumersPerClient(maxConsumersPerClient int) *EnvironmentOptions {
	envOptions.MaxConsumersPerClient = maxConsumersPerClient
	return envOptions

}

func (envOptions *EnvironmentOptions) SetUri(uri string) *EnvironmentOptions {
	envOptions.ConnectionParameters.Uri = uri
	return envOptions
}

func (envOptions *EnvironmentOptions) SetUser(user string) *EnvironmentOptions {
	envOptions.ConnectionParameters.User = user
	return envOptions
}

func (envOptions *EnvironmentOptions) SetPassword(password string) *EnvironmentOptions {
	envOptions.ConnectionParameters.Password = password
	return envOptions
}

func (envOptions *EnvironmentOptions) SetHost(host string) *EnvironmentOptions {
	envOptions.ConnectionParameters.Host = host
	return envOptions
}

func (envOptions *EnvironmentOptions) SetPort(port int) *EnvironmentOptions {
	envOptions.ConnectionParameters.Port = port
	return envOptions
}

func (envOptions *EnvironmentOptions) SetPublishErrorListener(
	publishErrorListener PublishErrorListener) *EnvironmentOptions {
	envOptions.PublishErrorListener = publishErrorListener
	return envOptions
}

type enviromentCoordinator struct {
	mutex             *sync.Mutex
	clientsPerContext map[int]*Client
	maxItemsForClient int
	nextId            int
}

func (cc *enviromentCoordinator) isProducerListFull(clientsPerContextId int) bool {
	return cc.clientsPerContext[clientsPerContextId].coordinator.
		ProducersCount() >= cc.maxItemsForClient
}

func (cc *enviromentCoordinator) isConsumerListFull(clientsPerContextId int) bool {
	return cc.clientsPerContext[clientsPerContextId].coordinator.
		ConsumersCount() >= cc.maxItemsForClient
}

func (cc *enviromentCoordinator) maybeCleanClients() {
	cc.mutex.Lock()
	defer cc.mutex.Unlock()
	for i, client := range cc.clientsPerContext {
		if !client.socket.isOpen() {
			delete(cc.clientsPerContext, i)
		}
	}
}

func (cc *enviromentCoordinator) maybeCleanProducers(streamName string) {
	cc.mutex.Lock()
	defer cc.mutex.Unlock()
	for _, client := range cc.clientsPerContext {
		for pidx, producer := range client.coordinator.producers {
			if producer.(*Producer).options.streamName == streamName {
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

func (cc *enviromentCoordinator) maybeCleanConsumers(streamName string) {
	cc.mutex.Lock()
	defer cc.mutex.Unlock()
	for _, client := range cc.clientsPerContext {
		for pidx, consumer := range client.coordinator.consumers {
			if consumer.(*Consumer).options.streamName == streamName {
				err := client.coordinator.RemoveConsumerById(pidx.(uint8))
				if err != nil {
					return
				}
			}
		}
		if client.coordinator.ConsumersCount() == 0 {
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

func (cc *enviromentCoordinator) newProducer(leader *Broker, streamName string, listener PublishErrorListener) (*Producer, error) {
	cc.mutex.Lock()
	defer cc.mutex.Unlock()
	var clientResult *Client
	for i, client := range cc.clientsPerContext {
		if !cc.isProducerListFull(i) {
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

func (cc *enviromentCoordinator) newConsumer(leader *Broker, streamName string, messagesHandler MessagesHandler, options *ConsumerOptions) (*Consumer, error) {
	cc.mutex.Lock()
	defer cc.mutex.Unlock()
	var clientResult *Client
	for i, client := range cc.clientsPerContext {
		if !cc.isConsumerListFull(i) {
			clientResult = client
			break
		}
	}

	if clientResult == nil {
		clientResult = NewClient()
		clientResult.broker = *leader
		clientResult.metadataListener = func(ch <-chan string) {
			streamName := <-ch
			cc.maybeCleanConsumers(streamName)
		}
		err := clientResult.connect()
		if err != nil {
			return nil, err
		}
		cc.nextId++
		cc.clientsPerContext[cc.nextId] = clientResult
	}

	subscriber, err := clientResult.DeclareSubscriber(streamName, messagesHandler, options)

	if err != nil {
		return nil, err
	}
	return subscriber, nil
}

func (cc *enviromentCoordinator) close() error {
	for _, client := range cc.clientsPerContext {
		err := client.Close()
		if err != nil {
			logWarn("Error during close the client, %s", err)
		}
	}
	return nil
}

func (cc *enviromentCoordinator) getClientsPerContext() map[int]*Client {
	cc.mutex.Lock()
	defer cc.mutex.Unlock()
	return cc.clientsPerContext
}

type producersEnvironment struct {
	mutex                *sync.Mutex
	producersCoordinator map[string]*enviromentCoordinator
	maxItemsForClient    int
	PublishErrorListener PublishErrorListener
}

func newProducers(maxItemsForClient int, publishErrorListener PublishErrorListener) *producersEnvironment {
	producers := &producersEnvironment{
		mutex:                &sync.Mutex{},
		producersCoordinator: map[string]*enviromentCoordinator{},
		maxItemsForClient:    maxItemsForClient,
		PublishErrorListener: publishErrorListener,
	}
	return producers
}

func (ps *producersEnvironment) NewProducer(clientLocator *Client, streamName string, producerOptions *ProducerOptions) (*Producer, error) {
	ps.mutex.Lock()
	defer ps.mutex.Unlock()
	leader, err := clientLocator.BrokerLeader(streamName)
	if err != nil {
		return nil, err
	}
	if ps.producersCoordinator[leader.hostPort()] == nil {
		ps.producersCoordinator[leader.hostPort()] = &enviromentCoordinator{
			clientsPerContext: map[int]*Client{},
			mutex:             &sync.Mutex{},
			maxItemsForClient: ps.maxItemsForClient,
			nextId:            0,
		}
	}
	leader.cloneFrom(clientLocator.broker)

	producer, err := ps.producersCoordinator[leader.hostPort()].newProducer(leader, streamName, ps.PublishErrorListener)
	if err != nil {
		return nil, err
	}
	producer.onClose = func(ch <-chan uint8) {
		for _, coordinator := range ps.producersCoordinator {
			coordinator.maybeCleanClients()
		}
	}
	producer.publishConfirm = producerOptions.PublishConfirmHandler

	return producer, err
}

func (ps *producersEnvironment) close() error {
	ps.mutex.Lock()
	defer ps.mutex.Unlock()
	for _, coordinator := range ps.producersCoordinator {
		_ = coordinator.close()
	}
	return nil
}

func (ps *producersEnvironment) getCoordinators() map[string]*enviromentCoordinator {
	ps.mutex.Lock()
	defer ps.mutex.Unlock()
	return ps.producersCoordinator
}

type consumersEnvironment struct {
	mutex                *sync.Mutex
	consumersCoordinator map[string]*enviromentCoordinator
	maxItemsForClient    int
	PublishErrorListener PublishErrorListener
}

func newConsumerEnvironment(maxItemsForClient int) *consumersEnvironment {
	producers := &consumersEnvironment{
		mutex:                &sync.Mutex{},
		consumersCoordinator: map[string]*enviromentCoordinator{},
		maxItemsForClient:    maxItemsForClient,
	}
	return producers
}

func (ps *consumersEnvironment) NewSubscriber(clientLocator *Client, streamName string, messagesHandler MessagesHandler, consumerOptions *ConsumerOptions) (*Consumer, error) {
	ps.mutex.Lock()
	defer ps.mutex.Unlock()
	leader, err := clientLocator.BrokerLeader(streamName)
	if err != nil {
		return nil, err
	}
	if ps.consumersCoordinator[leader.hostPort()] == nil {
		ps.consumersCoordinator[leader.hostPort()] = &enviromentCoordinator{
			clientsPerContext: map[int]*Client{},
			mutex:             &sync.Mutex{},
			maxItemsForClient: ps.maxItemsForClient,
			nextId:            0,
		}
	}
	leader.cloneFrom(clientLocator.broker)
	consumer, err := ps.consumersCoordinator[leader.hostPort()].
		newConsumer(leader, streamName, messagesHandler, consumerOptions)
	if err != nil {
		return nil, err
	}
	consumer.onClose = func(ch <-chan uint8) {
		for _, coordinator := range ps.consumersCoordinator {
			coordinator.maybeCleanClients()
		}
	}
	return consumer, err
}

func (ps *consumersEnvironment) close() error {
	ps.mutex.Lock()
	defer ps.mutex.Unlock()
	for _, coordinator := range ps.consumersCoordinator {
		_ = coordinator.close()
	}
	return nil
}

func (ps *consumersEnvironment) getCoordinators() map[string]*enviromentCoordinator {
	ps.mutex.Lock()
	defer ps.mutex.Unlock()
	return ps.consumersCoordinator
}
