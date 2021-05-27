package stream

import (
	"context"
	"net/url"
	"sync"
)

type Environment struct {
	producers *producersEnvironment
	consumers *consumersEnvironment
	options   *EnvironmentOptions
}

func NewEnvironment(options *EnvironmentOptions) (*Environment, error) {
	client := newClient("stream-locator")
	defer func(client *Client) {
		err := client.Close()
		if err != nil {
			return
		}
	}(client)
	if options == nil {
		options = NewEnvironmentOptions()
	}
	if options.MaxConsumersPerClient == 0 {
		options.MaxConsumersPerClient = 3
	}

	if options.MaxProducersPerClient == 0 {
		options.MaxProducersPerClient = 3
	}
	options.ConnectionParameters.mergeWithDefault()

	if options.ConnectionParameters.Uri != "" {
		u, err := url.Parse(options.ConnectionParameters.Uri)
		if err != nil {
			return nil, err
		}
		options.ConnectionParameters.User = u.User.Username()
		options.ConnectionParameters.Password, _ = u.User.Password()
	}

	client.broker = options.ConnectionParameters

	return &Environment{
		options:   options,
		producers: newProducers(options.MaxProducersPerClient),
		consumers: newConsumerEnvironment(options.MaxConsumersPerClient),
	}, client.connect()
}
func (env *Environment) newClientLocator() (*Client, error) {
	client := newClient("stream-locator")
	client.broker = env.options.ConnectionParameters
	return client, client.connect()
}

func (env *Environment) DeclareStream(streamName string, options *StreamOptions) error {
	client, err := env.newClientLocator()
	defer func(client *Client) {
		err := client.Close()
		if err != nil {
			return
		}
	}(client)
	if err != nil {
		return err
	}
	return client.DeclareStream(streamName, options)
}

func (env *Environment) DeleteStream(streamName string) error {
	client, err := env.newClientLocator()
	defer func(client *Client) {
		err := client.Close()
		if err != nil {
			return
		}
	}(client)
	if err != nil {
		return err
	}
	return client.DeleteStream(streamName)
}

func (env *Environment) NewProducer(streamName string, producerOptions *ProducerOptions) (*Producer, error) {
	client, err := env.newClientLocator()
	defer func(client *Client) {
		err := client.Close()
		if err != nil {
			return
		}
	}(client)
	if err != nil {
		return nil, err
	}
	if producerOptions == nil {
		producerOptions = NewProducerOptions()
	}
	return env.producers.newProducer(client, streamName, producerOptions)
}

func (env *Environment) StreamMetaData(streamName string) (*StreamMetadata, error) {
	client, err := env.newClientLocator()
	defer func(client *Client) {
		err := client.Close()
		if err != nil {
			return
		}
	}(client)
	if err != nil {
		return nil, err
	}
	streamsMetadata := client.metaData(streamName)
	streamMetadata := streamsMetadata.Get(streamName)
	return streamMetadata, nil
}

func (env *Environment) NewConsumer(ctx context.Context, streamName string,
	messagesHandler MessagesHandler,
	options *ConsumerOptions) (*Consumer, error) {
	client, err := env.newClientLocator()
	defer func(client *Client) {
		err := client.Close()
		if err != nil {
			return
		}
	}(client)
	if err != nil {
		return nil, err
	}
	if options == nil {
		options = NewConsumerOptions()
	}
	return env.consumers.NewSubscriber(ctx, client, streamName, messagesHandler, options)
}

func (env *Environment) Close() error {
	_ = env.producers.close()
	_ = env.consumers.close()
	return nil
}

type EnvironmentOptions struct {
	ConnectionParameters  Broker
	MaxProducersPerClient int
	MaxConsumersPerClient int
}

func NewEnvironmentOptions() *EnvironmentOptions {
	return &EnvironmentOptions{
		MaxProducersPerClient: 1,
		MaxConsumersPerClient: 1,
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
			if producer.(*Producer).GetStreamName() == streamName {
				err := client.coordinator.RemoveProducerById(pidx.(uint8), Event{
					Command:    CommandMetadataUpdate,
					StreamName: streamName,
					Name:       producer.(*Producer).GetName(),
					Reason:     "Meta data update",
					Err:        nil,
				})
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
				err := client.coordinator.RemoveConsumerById(pidx.(uint8), Event{
					Command:    CommandMetadataUpdate,
					StreamName: streamName,
					Name:       consumer.(*Consumer).GetName(),
					Reason:     "Meta data update",
					Err:        nil,
				})
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

func (cc *enviromentCoordinator) newProducer(leader *Broker, streamName string,
	channelPublishErrListener ChannelPublishError,
	options *ProducerOptions) (*Producer, error) {
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
		clientResult = newClient("stream-producers")
		clientResult.broker = *leader
		chMeta := make(chan string)
		clientResult.metadataListener = chMeta
		go func(ch <-chan string, cl *Client) {
			for {
				streamName := <-ch
				cc.maybeCleanProducers(streamName)
				if !cl.socket.isOpen() {
					return
				}
			}

		}(chMeta, clientResult)

		clientResult.publishErrorListener = channelPublishErrListener

		cc.nextId++
		cc.clientsPerContext[cc.nextId] = clientResult
	}

	err := clientResult.connect()
	if err != nil {
		return nil, err
	}

	publisher, err := clientResult.DeclarePublisher(streamName, options)

	if err != nil {
		return nil, err
	}

	return publisher, nil
}

func (cc *enviromentCoordinator) newConsumer(ctx context.Context, leader *Broker,
	streamName string, messagesHandler MessagesHandler,
	options *ConsumerOptions) (*Consumer, error) {
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
		clientResult = newClient("stream-consumer")
		clientResult.broker = *leader
		chMeta := make(chan string)
		clientResult.metadataListener = chMeta
		go func(ch <-chan string, cl *Client) {
			for {
				<-ch
				cc.maybeCleanConsumers(streamName)
				if !cl.socket.isOpen() {
					return
				}
			}

		}(chMeta, clientResult)

		cc.nextId++
		cc.clientsPerContext[cc.nextId] = clientResult
	}
	// try to reconnect in case the socket is closed
	err := clientResult.connect()
	if err != nil {
		return nil, err
	}

	subscriber, err := clientResult.DeclareSubscriber(ctx, streamName, messagesHandler, options)

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
	publishErrorListener ChannelPublishError
}

func newProducers(maxItemsForClient int) *producersEnvironment {
	producers := &producersEnvironment{
		mutex:                &sync.Mutex{},
		producersCoordinator: map[string]*enviromentCoordinator{},
		maxItemsForClient:    maxItemsForClient,
	}
	return producers
}

func (ps *producersEnvironment) newProducer(clientLocator *Client, streamName string,
	options *ProducerOptions) (*Producer, error) {
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

	producer, err := ps.producersCoordinator[leader.hostPort()].newProducer(leader, streamName,
		ps.publishErrorListener,
		options)
	if err != nil {
		return nil, err
	}
	producer.onClose = func(ch <-chan uint8) {
		for _, coordinator := range ps.producersCoordinator {
			coordinator.maybeCleanClients()
		}
	}

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
	PublishErrorListener ChannelPublishError
}

func newConsumerEnvironment(maxItemsForClient int) *consumersEnvironment {
	producers := &consumersEnvironment{
		mutex:                &sync.Mutex{},
		consumersCoordinator: map[string]*enviromentCoordinator{},
		maxItemsForClient:    maxItemsForClient,
	}
	return producers
}

func (ps *consumersEnvironment) NewSubscriber(ctx context.Context, clientLocator *Client, streamName string,
	messagesHandler MessagesHandler,
	consumerOptions *ConsumerOptions) (*Consumer, error) {
	ps.mutex.Lock()
	defer ps.mutex.Unlock()
	consumerBroker, err := clientLocator.BrokerForConsumer(streamName)
	if err != nil {
		return nil, err
	}
	if ps.consumersCoordinator[consumerBroker.hostPort()] == nil {
		ps.consumersCoordinator[consumerBroker.hostPort()] = &enviromentCoordinator{
			clientsPerContext: map[int]*Client{},
			mutex:             &sync.Mutex{},
			maxItemsForClient: ps.maxItemsForClient,
			nextId:            0,
		}
	}
	consumerBroker.cloneFrom(clientLocator.broker)
	consumer, err := ps.consumersCoordinator[consumerBroker.hostPort()].
		newConsumer(ctx, consumerBroker, streamName, messagesHandler, consumerOptions)
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
