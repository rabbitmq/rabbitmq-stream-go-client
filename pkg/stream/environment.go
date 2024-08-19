package stream

import (
	"crypto/tls"
	"errors"
	"fmt"
	"math/rand"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/logs"
)

type Environment struct {
	producers *producersEnvironment
	consumers *consumersEnvironment
	options   *EnvironmentOptions
	closed    bool
}

func NewEnvironment(options *EnvironmentOptions) (*Environment, error) {
	if options == nil {
		options = NewEnvironmentOptions()
	}

	if options.RPCTimeout <= 0 {
		options.RPCTimeout = defaultSocketCallTimeout
	}

	client := newClient("go-stream-locator", nil,
		options.TCPParameters, options.SaslConfiguration, options.RPCTimeout)
	defer func(client *Client) {
		err := client.Close()
		if err != nil {
			return
		}
	}(client)

	if options.MaxConsumersPerClient <= 0 || options.MaxProducersPerClient <= 0 ||
		options.MaxConsumersPerClient > 254 || options.MaxProducersPerClient > 254 {
		return nil, fmt.Errorf(" MaxConsumersPerClient and MaxProducersPerClient must be between 1 and 254")
	}

	if options.SaslConfiguration != nil {
		if options.SaslConfiguration.Mechanism != SaslConfigurationPlain && options.SaslConfiguration.Mechanism != SaslConfigurationExternal {
			return nil, fmt.Errorf("SaslConfiguration mechanism must be PLAIN or EXTERNAL")
		}
	}

	if len(options.ConnectionParameters) == 0 {
		options.ConnectionParameters = []*Broker{newBrokerDefault()}
	}
	var connectionError error
	for idx, parameter := range options.ConnectionParameters {

		if parameter.Uri != "" {
			u, err := url.Parse(parameter.Uri)
			if err != nil {
				return nil, err
			}
			parameter.Scheme = u.Scheme
			parameter.User = u.User.Username()
			parameter.Password, _ = u.User.Password()
			parameter.Host = u.Host
			parameter.Port = u.Port()

			if vhost := strings.TrimPrefix(u.Path, "/"); len(vhost) > 0 {
				if vhost != "/" && strings.Contains(vhost, "/") {
					return nil, errors.New("multiple segments in URI path: " + u.Path)
				}
				parameter.Vhost = vhost
			}
		}

		parameter.mergeWithDefault()

		client.broker = parameter

		connectionError = client.connect()
		if connectionError == nil {
			break
		} else {
			nextIfThereIs := ""
			if idx < len(options.ConnectionParameters)-1 {
				nextIfThereIs = "Trying the next broker..."
			}
			logs.LogError("New environment creation. Can't connect to the broker: %s port: %s. %s",
				parameter.Host, parameter.Port, nextIfThereIs)

		}
	}

	return &Environment{
		options:   options,
		producers: newProducers(options.MaxProducersPerClient),
		consumers: newConsumerEnvironment(options.MaxConsumersPerClient),
		closed:    false,
	}, connectionError
}
func (env *Environment) newReconnectClient() (*Client, error) {
	broker := env.options.ConnectionParameters[0]
	client := newClient("go-stream-locator", broker, env.options.TCPParameters,
		env.options.SaslConfiguration, env.options.RPCTimeout)

	err := client.connect()
	tentatives := 1
	for err != nil {
		sleepTime := rand.Intn(5000) + (tentatives * 1000)

		brokerUri := fmt.Sprintf("%s://%s:***@%s:%s/%s", client.broker.Scheme, client.broker.User, client.broker.Host, client.broker.Port, client.broker.Vhost)
		logs.LogError("Can't connect the locator client, error:%s, retry in %d milliseconds, broker: ", err, sleepTime, brokerUri)

		time.Sleep(time.Duration(sleepTime) * time.Millisecond)
		rand.Seed(time.Now().UnixNano())
		n := rand.Intn(len(env.options.ConnectionParameters))
		client = newClient("stream-locator", env.options.ConnectionParameters[n], env.options.TCPParameters,
			env.options.SaslConfiguration, env.options.RPCTimeout)
		tentatives = tentatives + 1
		err = client.connect()

	}

	return client, client.connect()
}

func (env *Environment) DeclareStream(streamName string, options *StreamOptions) error {
	client, err := env.newReconnectClient()
	defer func(client *Client) {
		_ = client.Close()
	}(client)
	if err != nil {
		return err
	}
	if err := client.DeclareStream(streamName, options); err != nil && err != StreamAlreadyExists {
		return err
	}
	return nil
}

func (env *Environment) DeleteStream(streamName string) error {
	client, err := env.newReconnectClient()
	defer func(client *Client) {
		_ = client.Close()
	}(client)
	if err != nil {
		return err
	}
	return client.DeleteStream(streamName)
}

func (env *Environment) NewProducer(streamName string, producerOptions *ProducerOptions) (*Producer, error) {
	client, err := env.newReconnectClient()
	defer func(client *Client) {
		_ = client.Close()
	}(client)
	if err != nil {
		return nil, err
	}

	return env.producers.newProducer(client, streamName, producerOptions, env.options.AddressResolver, env.options.RPCTimeout)
}

func (env *Environment) StreamExists(streamName string) (bool, error) {
	client, err := env.newReconnectClient()
	defer func(client *Client) {
		_ = client.Close()
	}(client)
	if err != nil {
		return false, err
	}
	return client.StreamExists(streamName), nil
}

func (env *Environment) QueryOffset(consumerName string, streamName string) (int64, error) {
	client, err := env.newReconnectClient()
	defer func(client *Client) {
		_ = client.Close()
	}(client)
	if err != nil {
		return 0, err
	}
	return client.queryOffset(consumerName, streamName)
}

// QuerySequence gets the last id stored for a producer
// you can also see producer.GetLastPublishingId() that is the easier way to get the last-id
func (env *Environment) QuerySequence(publisherReference string, streamName string) (int64, error) {
	client, err := env.newReconnectClient()
	defer func(client *Client) {
		_ = client.Close()
	}(client)
	if err != nil {
		return 0, err
	}
	return client.queryPublisherSequence(publisherReference, streamName)
}

func (env *Environment) StreamStats(streamName string) (*StreamStats, error) {
	client, err := env.newReconnectClient()
	defer func(client *Client) {
		_ = client.Close()
	}(client)
	if err != nil {
		return nil, err
	}
	return client.StreamStats(streamName)
}

func (env *Environment) StreamMetaData(streamName string) (*StreamMetadata, error) {
	client, err := env.newReconnectClient()
	defer func(client *Client) {
		_ = client.Close()
	}(client)
	if err != nil {
		return nil, err
	}
	streamsMetadata := client.metaData(streamName)
	streamMetadata := streamsMetadata.Get(streamName)
	if streamMetadata.responseCode != responseCodeOk {
		return nil, lookErrorCode(streamMetadata.responseCode)
	}

	tentatives := 0
	for streamMetadata == nil || streamMetadata.Leader == nil && tentatives < 3 {
		streamsMetadata = client.metaData(streamName)
		streamMetadata = streamsMetadata.Get(streamName)
		tentatives++
		time.Sleep(100 * time.Millisecond)
	}

	if streamMetadata.Leader == nil {

		return nil, LeaderNotReady
	}

	return streamMetadata, nil
}

func (env *Environment) NewConsumer(streamName string,
	messagesHandler MessagesHandler,
	options *ConsumerOptions) (*Consumer, error) {
	client, err := env.newReconnectClient()
	defer func(client *Client) {
		_ = client.Close()
	}(client)
	if err != nil {
		return nil, err
	}

	return env.consumers.NewSubscriber(client, streamName, messagesHandler, options, env.options.AddressResolver, env.options.RPCTimeout)
}

func (env *Environment) NewSuperStreamProducer(superStream string, superStreamProducerOptions *SuperStreamProducerOptions) (*SuperStreamProducer, error) {
	var p, err = newSuperStreamProducer(env, superStream, superStreamProducerOptions)
	if err != nil {
		return nil, err
	}
	return p, p.init()
}

func (env *Environment) Close() error {
	_ = env.producers.close()
	_ = env.consumers.close()
	env.closed = true
	return nil
}

func (env *Environment) IsClosed() bool {
	return env.closed
}

type EnvironmentOptions struct {
	ConnectionParameters  []*Broker
	TCPParameters         *TCPParameters
	SaslConfiguration     *SaslConfiguration
	MaxProducersPerClient int
	MaxConsumersPerClient int
	AddressResolver       *AddressResolver
	RPCTimeout            time.Duration
}

func NewEnvironmentOptions() *EnvironmentOptions {
	return &EnvironmentOptions{
		MaxProducersPerClient: 1,
		MaxConsumersPerClient: 1,
		ConnectionParameters:  []*Broker{},
		TCPParameters:         newTCPParameterDefault(),
		SaslConfiguration:     newSaslConfigurationDefault(),
		RPCTimeout:            defaultSocketCallTimeout,
	}
}

func (envOptions *EnvironmentOptions) SetAddressResolver(addressResolver AddressResolver) *EnvironmentOptions {
	envOptions.AddressResolver = &AddressResolver{
		Host: addressResolver.Host,
		Port: addressResolver.Port,
	}
	return envOptions
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
	if len(envOptions.ConnectionParameters) == 0 {
		envOptions.ConnectionParameters = append(envOptions.ConnectionParameters, &Broker{Uri: uri})
	} else {
		envOptions.ConnectionParameters[0].Uri = uri
	}

	return envOptions
}

func (envOptions *EnvironmentOptions) SetUris(uris []string) *EnvironmentOptions {
	for _, s := range uris {
		envOptions.ConnectionParameters = append(envOptions.ConnectionParameters, &Broker{Uri: s})
	}
	return envOptions
}

func (envOptions *EnvironmentOptions) SetHost(host string) *EnvironmentOptions {
	if len(envOptions.ConnectionParameters) == 0 {
		envOptions.ConnectionParameters = append(envOptions.ConnectionParameters, &Broker{Host: host})
	} else {
		envOptions.ConnectionParameters[0].Host = host
	}
	return envOptions
}

func (envOptions *EnvironmentOptions) SetVHost(vhost string) *EnvironmentOptions {
	if len(envOptions.ConnectionParameters) == 0 {
		envOptions.ConnectionParameters = append(envOptions.ConnectionParameters, &Broker{Vhost: vhost})
	} else {
		envOptions.ConnectionParameters[0].Vhost = vhost
	}
	return envOptions
}

func (envOptions *EnvironmentOptions) SetSaslConfiguration(value string) *EnvironmentOptions {
	if envOptions.SaslConfiguration == nil {
		envOptions.SaslConfiguration = newSaslConfigurationDefault()
	}
	envOptions.SaslConfiguration.Mechanism = value
	return envOptions
}

func (envOptions *EnvironmentOptions) SetTLSConfig(config *tls.Config) *EnvironmentOptions {
	if envOptions.TCPParameters == nil {
		envOptions.TCPParameters = newTCPParameterDefault()
	}
	envOptions.TCPParameters.tlsConfig = config
	return envOptions
}

func (envOptions *EnvironmentOptions) IsTLS(val bool) *EnvironmentOptions {
	if val {
		if len(envOptions.ConnectionParameters) == 0 {
			envOptions.ConnectionParameters = append(envOptions.ConnectionParameters, &Broker{Scheme: "rabbitmq-stream+tls"})
		} else {
			for _, parameter := range envOptions.ConnectionParameters {
				parameter.Scheme = "rabbitmq-stream+tls"
			}
		}
	}
	return envOptions
}

func (envOptions *EnvironmentOptions) SetPort(port int) *EnvironmentOptions {
	if len(envOptions.ConnectionParameters) == 0 {
		brokerOptions := newBrokerDefault()
		brokerOptions.Port = strconv.Itoa(port)
		envOptions.ConnectionParameters = append(envOptions.ConnectionParameters, brokerOptions)
	} else {
		envOptions.ConnectionParameters[0].Port = strconv.Itoa(port)
	}
	return envOptions

}

func (envOptions *EnvironmentOptions) SetUser(user string) *EnvironmentOptions {
	if len(envOptions.ConnectionParameters) == 0 {
		envOptions.ConnectionParameters = append(envOptions.ConnectionParameters, &Broker{User: user})
	} else {
		envOptions.ConnectionParameters[0].User = user
	}
	return envOptions

}

func (envOptions *EnvironmentOptions) SetPassword(password string) *EnvironmentOptions {
	if len(envOptions.ConnectionParameters) == 0 {
		envOptions.ConnectionParameters = append(envOptions.ConnectionParameters, &Broker{Password: password})
	} else {
		envOptions.ConnectionParameters[0].Password = password
	}
	return envOptions

}

func (envOptions *EnvironmentOptions) SetRequestedHeartbeat(requestedHeartbeat time.Duration) *EnvironmentOptions {
	if envOptions.TCPParameters == nil {
		envOptions.TCPParameters = newTCPParameterDefault()
	}
	envOptions.TCPParameters.RequestedHeartbeat = requestedHeartbeat

	return envOptions
}

func (envOptions *EnvironmentOptions) SetRequestedMaxFrameSize(requestedMaxFrameSize int) *EnvironmentOptions {
	if envOptions.TCPParameters == nil {
		envOptions.TCPParameters = newTCPParameterDefault()
	}
	envOptions.TCPParameters.RequestedMaxFrameSize = requestedMaxFrameSize

	return envOptions
}

func (envOptions *EnvironmentOptions) SetWriteBuffer(writeBuffer int) *EnvironmentOptions {
	if envOptions.TCPParameters == nil {
		envOptions.TCPParameters = newTCPParameterDefault()
	}
	envOptions.TCPParameters.WriteBuffer = writeBuffer

	return envOptions
}

func (envOptions *EnvironmentOptions) SetReadBuffer(readBuffer int) *EnvironmentOptions {
	if envOptions.TCPParameters == nil {
		envOptions.TCPParameters = newTCPParameterDefault()
	}
	envOptions.TCPParameters.ReadBuffer = readBuffer

	return envOptions
}

func (envOptions *EnvironmentOptions) SetNoDelay(noDelay bool) *EnvironmentOptions {
	if envOptions.TCPParameters == nil {
		envOptions.TCPParameters = newTCPParameterDefault()
	}
	envOptions.TCPParameters.NoDelay = noDelay

	return envOptions
}

func (envOptions *EnvironmentOptions) SetRPCTimeout(timeout time.Duration) *EnvironmentOptions {
	envOptions.RPCTimeout = timeout
	return envOptions
}

type environmentCoordinator struct {
	mutex             *sync.Mutex
	mutexContext      *sync.RWMutex
	clientsPerContext map[int]*Client
	maxItemsForClient int
	nextId            int
}

func (cc *environmentCoordinator) isProducerListFull(clientsPerContextId int) bool {
	return cc.clientsPerContext[clientsPerContextId].coordinator.
		ProducersCount() >= cc.maxItemsForClient
}

func (cc *environmentCoordinator) isConsumerListFull(clientsPerContextId int) bool {
	return cc.clientsPerContext[clientsPerContextId].coordinator.
		ConsumersCount() >= cc.maxItemsForClient
}

func (cc *environmentCoordinator) maybeCleanClients() {
	cc.mutex.Lock()
	defer cc.mutex.Unlock()
	cc.mutexContext.Lock()
	defer cc.mutexContext.Unlock()
	for i, client := range cc.clientsPerContext {
		if !client.socket.isOpen() {
			delete(cc.clientsPerContext, i)
		}
	}
}

func (c *Client) maybeCleanProducers(streamName string) {
	c.mutex.Lock()
	for pidx, producer := range c.coordinator.Producers() {
		if producer.(*Producer).GetStreamName() == streamName {
			err := c.coordinator.RemoveProducerById(pidx.(uint8), Event{
				Command:    CommandMetadataUpdate,
				StreamName: streamName,
				Name:       producer.(*Producer).GetName(),
				Reason:     MetaDataUpdate,
				Err:        nil,
			})
			if err != nil {
				return
			}
		}
	}
	c.mutex.Unlock()
	if c.coordinator.ProducersCount() == 0 {
		err := c.Close()
		if err != nil {
			return
		}
	}
}

func (c *Client) maybeCleanConsumers(streamName string) {
	c.mutex.Lock()
	for pidx, consumer := range c.coordinator.consumers {
		if consumer.(*Consumer).options.streamName == streamName {
			err := c.coordinator.RemoveConsumerById(pidx.(uint8), Event{
				Command:    CommandMetadataUpdate,
				StreamName: streamName,
				Name:       consumer.(*Consumer).GetName(),
				Reason:     MetaDataUpdate,
				Err:        nil,
			})
			if err != nil {
				return
			}
		}
	}
	c.mutex.Unlock()
	if c.coordinator.ConsumersCount() == 0 {
		err := c.Close()
		if err != nil {
			return
		}
	}
}

func (cc *environmentCoordinator) newProducer(leader *Broker, tcpParameters *TCPParameters, saslConfiguration *SaslConfiguration, streamName string,
	options *ProducerOptions, rpcTimeout time.Duration) (*Producer, error) {
	cc.mutex.Lock()
	defer cc.mutex.Unlock()
	cc.mutexContext.Lock()
	defer cc.mutexContext.Unlock()
	var clientResult *Client
	for i, client := range cc.clientsPerContext {
		if !cc.isProducerListFull(i) {
			clientResult = client
			break
		}
	}
	clientProvidedName := "go-stream-producer"
	if options != nil && options.ClientProvidedName != "" {
		clientProvidedName = options.ClientProvidedName
	}

	if clientResult == nil {
		clientResult = cc.newClientForProducer(clientProvidedName, leader, tcpParameters, saslConfiguration, rpcTimeout)
	}

	err := clientResult.connect()
	if err != nil {
		return nil, err
	}

	for clientResult.connectionProperties.host != leader.advHost ||
		clientResult.connectionProperties.port != leader.advPort {
		logs.LogDebug("connectionProperties host %s doesn't match with the advertised_host %s, advertised_port %s .. retry",
			clientResult.connectionProperties.host,
			leader.advHost, leader.advPort)
		err := clientResult.Close()
		if err != nil {
			return nil, err
		}
		clientResult = cc.newClientForProducer(clientProvidedName, leader, tcpParameters, saslConfiguration, rpcTimeout)
		err = clientResult.connect()
		if err != nil {
			return nil, err
		}
		time.Sleep(1 * time.Second)
	}

	producer, err := clientResult.DeclarePublisher(streamName, options)

	if err != nil {
		return nil, err
	}

	return producer, nil
}

func (cc *environmentCoordinator) newClientForProducer(connectionName string, leader *Broker, tcpParameters *TCPParameters, saslConfiguration *SaslConfiguration, rpcTimeOut time.Duration) *Client {
	clientResult := newClient(connectionName, leader, tcpParameters, saslConfiguration, rpcTimeOut)
	chMeta := make(chan metaDataUpdateEvent, 1)
	clientResult.metadataListener = chMeta
	go func(ch <-chan metaDataUpdateEvent, cl *Client) {
		for metaDataUpdateEvent := range ch {
			clientResult.maybeCleanProducers(metaDataUpdateEvent.StreamName)
			cc.maybeCleanClients()
			if !cl.socket.isOpen() {
				return
			}
		}

	}(chMeta, clientResult)

	cc.nextId++
	cc.clientsPerContext[cc.nextId] = clientResult
	return clientResult
}

func (cc *environmentCoordinator) newConsumer(connectionName string, leader *Broker, tcpParameters *TCPParameters, saslConfiguration *SaslConfiguration,
	streamName string, messagesHandler MessagesHandler,
	options *ConsumerOptions, rpcTimeout time.Duration) (*Consumer, error) {
	cc.mutex.Lock()
	defer cc.mutex.Unlock()
	cc.mutexContext.Lock()
	defer cc.mutexContext.Unlock()
	var clientResult *Client
	for i, client := range cc.clientsPerContext {
		if !cc.isConsumerListFull(i) {
			clientResult = client
			break
		}
	}

	if clientResult == nil {
		clientResult = newClient(connectionName, leader, tcpParameters, saslConfiguration, rpcTimeout)
		chMeta := make(chan metaDataUpdateEvent)
		clientResult.metadataListener = chMeta
		go func(ch <-chan metaDataUpdateEvent, cl *Client) {
			for metaDataUpdateEvent := range ch {
				clientResult.maybeCleanConsumers(metaDataUpdateEvent.StreamName)
				cc.maybeCleanClients()
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

	subscriber, err := clientResult.DeclareSubscriber(streamName, messagesHandler, options)

	if err != nil {
		return nil, err
	}
	return subscriber, nil
}

func (cc *environmentCoordinator) Close() error {
	cc.mutexContext.Lock()
	defer cc.mutexContext.Unlock()
	for _, client := range cc.clientsPerContext {
		err := client.Close()
		if err != nil {
			logs.LogWarn("Error during close the client, %s", err)
		}
	}
	return nil
}

func (cc *environmentCoordinator) getClientsPerContext() map[int]*Client {
	cc.mutexContext.Lock()
	defer cc.mutexContext.Unlock()
	return cc.clientsPerContext
}

type producersEnvironment struct {
	mutex                *sync.Mutex
	producersCoordinator map[string]*environmentCoordinator
	maxItemsForClient    int
}

func newProducers(maxItemsForClient int) *producersEnvironment {
	producers := &producersEnvironment{
		mutex:                &sync.Mutex{},
		producersCoordinator: map[string]*environmentCoordinator{},
		maxItemsForClient:    maxItemsForClient,
	}
	return producers
}

func (ps *producersEnvironment) newProducer(clientLocator *Client, streamName string,
	options *ProducerOptions, resolver *AddressResolver, rpcTimeOut time.Duration) (*Producer, error) {
	ps.mutex.Lock()
	defer ps.mutex.Unlock()
	leader, err := clientLocator.BrokerLeader(streamName)
	if err != nil {
		return nil, err
	}
	coordinatorKey := leader.hostPort()
	if ps.producersCoordinator[coordinatorKey] == nil {
		ps.producersCoordinator[coordinatorKey] = &environmentCoordinator{
			clientsPerContext: map[int]*Client{},
			mutex:             &sync.Mutex{},
			maxItemsForClient: ps.maxItemsForClient,
			mutexContext:      &sync.RWMutex{},
			nextId:            0,
		}
	}
	leader.cloneFrom(clientLocator.broker, resolver)

	producer, err := ps.producersCoordinator[coordinatorKey].newProducer(leader, clientLocator.tcpParameters,
		clientLocator.saslConfiguration, streamName, options, rpcTimeOut)
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
		_ = coordinator.Close()
	}
	return nil
}

func (ps *producersEnvironment) getCoordinators() map[string]*environmentCoordinator {
	ps.mutex.Lock()
	defer ps.mutex.Unlock()
	return ps.producersCoordinator
}

type consumersEnvironment struct {
	mutex                *sync.Mutex
	consumersCoordinator map[string]*environmentCoordinator
	maxItemsForClient    int
}

func newConsumerEnvironment(maxItemsForClient int) *consumersEnvironment {
	producers := &consumersEnvironment{
		mutex:                &sync.Mutex{},
		consumersCoordinator: map[string]*environmentCoordinator{},
		maxItemsForClient:    maxItemsForClient,
	}
	return producers
}

func (ps *consumersEnvironment) NewSubscriber(clientLocator *Client, streamName string,
	messagesHandler MessagesHandler,
	consumerOptions *ConsumerOptions, resolver *AddressResolver, rpcTimeout time.Duration) (*Consumer, error) {
	ps.mutex.Lock()
	defer ps.mutex.Unlock()
	consumerBroker, err := clientLocator.BrokerForConsumer(streamName)
	if err != nil {
		return nil, err
	}
	coordinatorKey := consumerBroker.hostPort()
	if ps.consumersCoordinator[coordinatorKey] == nil {
		ps.consumersCoordinator[coordinatorKey] = &environmentCoordinator{
			clientsPerContext: map[int]*Client{},
			mutex:             &sync.Mutex{},
			maxItemsForClient: ps.maxItemsForClient,
			mutexContext:      &sync.RWMutex{},
			nextId:            0,
		}
	}
	consumerBroker.cloneFrom(clientLocator.broker, resolver)
	clientProvidedName := "go-stream-consumer"
	if consumerOptions != nil && consumerOptions.ClientProvidedName != "" {
		clientProvidedName = consumerOptions.ClientProvidedName
	}
	consumer, err := ps.consumersCoordinator[coordinatorKey].
		newConsumer(clientProvidedName, consumerBroker, clientLocator.tcpParameters,
			clientLocator.saslConfiguration,
			streamName, messagesHandler, consumerOptions, rpcTimeout)
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
		_ = coordinator.Close()
	}
	return nil
}

func (ps *consumersEnvironment) getCoordinators() map[string]*environmentCoordinator {
	ps.mutex.Lock()
	defer ps.mutex.Unlock()
	return ps.consumersCoordinator
}

// Super stream

func (env *Environment) DeclareSuperStream(superStreamName string, options SuperStreamOptions) error {
	client, err := env.newReconnectClient()
	defer func(client *Client) {
		_ = client.Close()
	}(client)
	if err != nil {
		return err
	}
	if err := client.DeclareSuperStream(superStreamName, options); err != nil && !errors.Is(err, StreamAlreadyExists) {
		return err
	}
	return nil
}

func (env *Environment) DeleteSuperStream(superStreamName string) error {
	client, err := env.newReconnectClient()
	defer func(client *Client) {
		_ = client.Close()
	}(client)
	if err != nil {
		return err
	}
	return client.DeleteSuperStream(superStreamName)
}

func (env *Environment) QueryPartitions(superStreamName string) ([]string, error) {
	client, err := env.newReconnectClient()
	defer func(client *Client) {
		_ = client.Close()
	}(client)
	if err != nil {
		return nil, err
	}
	return client.QueryPartitions(superStreamName)
}

func (env *Environment) QueryRoute(superStream string, routingKey string) ([]string, error) {
	client, err := env.newReconnectClient()
	defer func(client *Client) {
		_ = client.Close()
	}(client)
	if err != nil {
		return nil, err
	}
	return client.queryRoute(superStream, routingKey)
}

func (env *Environment) NewSuperStreamConsumer(superStream string, messagesHandler MessagesHandler, options *SuperStreamConsumerOptions) (*SuperStreamConsumer, error) {
	s, err := newSuperStreamConsumer(env, superStream, messagesHandler, options)
	if err != nil {
		return nil, err
	}
	err = s.init()
	return s, err
}
