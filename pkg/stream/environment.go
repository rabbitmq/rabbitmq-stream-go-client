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

type locator struct {
	client *Client
	mutex  sync.Mutex
}

func newLocator(client *Client) *locator {
	return &locator{
		client: client,
		mutex:  sync.Mutex{},
	}
}

type Environment struct {
	producers *producersEnvironment
	consumers *consumersEnvironment
	options   *EnvironmentOptions
	locator   *locator
	closed    bool
}

func NewEnvironment(options *EnvironmentOptions) (*Environment, error) {
	if options == nil {
		options = NewEnvironmentOptions()
	}

	if options.RPCTimeout <= 0 {
		options.RPCTimeout = defaultSocketCallTimeout
	}

	if options.TCPParameters == nil {
		options.TCPParameters = newTCPParameterDefault()
	}

	client := newClient(connectionParameters{
		connectionName:    "go-stream-locator",
		broker:            nil,
		tcpParameters:     options.TCPParameters,
		saslConfiguration: options.SaslConfiguration,
		rpcTimeOut:        options.RPCTimeout,
	})
	defer client.Close()

	// we put a limit to the heartbeat.
	// it doesn't make sense to have a heartbeat less than 3 seconds
	if options.TCPParameters.RequestedHeartbeat < (3 * time.Second) {
		return nil, errors.New("RequestedHeartbeat must be greater than 3 seconds")
	}

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
		locator:   newLocator(client),
	}, connectionError
}
func (env *Environment) maybeReconnectLocator() error {
	env.locator.mutex.Lock()
	defer env.locator.mutex.Unlock()
	if env.locator.client != nil && env.locator.client.socket.isOpen() {
		return nil
	}

	broker := env.options.ConnectionParameters[0]
	c := newClient(connectionParameters{
		connectionName:    "go-stream-locator",
		broker:            broker,
		tcpParameters:     env.options.TCPParameters,
		saslConfiguration: env.options.SaslConfiguration,
		rpcTimeOut:        env.options.RPCTimeout,
	})

	env.locator.client = c
	err := c.connect()
	tentatives := 1
	for err != nil {
		sleepTime := rand.Intn(5000) + (tentatives * 1000)

		brokerUri := fmt.Sprintf("%s://%s:***@%s:%s/%s", c.broker.Scheme, c.broker.User, c.broker.Host, c.broker.Port, c.broker.Vhost)
		logs.LogError("Can't connect the locator client, error:%s, retry in %d milliseconds, broker: %s", err, sleepTime, brokerUri)

		time.Sleep(time.Duration(sleepTime) * time.Millisecond)
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		n := r.Intn(len(env.options.ConnectionParameters))
		c1 := newClient(connectionParameters{
			connectionName:    "go-stream-locator",
			broker:            env.options.ConnectionParameters[n],
			tcpParameters:     env.options.TCPParameters,
			saslConfiguration: env.options.SaslConfiguration,
			rpcTimeOut:        env.options.RPCTimeout,
		})
		tentatives++
		env.locator.client = c1
		err = c1.connect()
	}

	return env.locator.client.connect()
}

func (env *Environment) DeclareStream(streamName string, options *StreamOptions) error {
	err := env.maybeReconnectLocator()
	if err != nil {
		return err
	}
	if err := env.locator.client.DeclareStream(streamName, options); err != nil && err != StreamAlreadyExists {
		return err
	}
	return nil
}

func (env *Environment) DeleteStream(streamName string) error {
	err := env.maybeReconnectLocator()
	if err != nil {
		return err
	}
	return env.locator.client.DeleteStream(streamName)
}

func (env *Environment) NewProducer(streamName string, producerOptions *ProducerOptions) (*Producer, error) {
	err := env.maybeReconnectLocator()

	if err != nil {
		return nil, err
	}

	return env.producers.newProducer(env.locator.client, streamName, producerOptions, env.options.AddressResolver, env.options.RPCTimeout)
}

func (env *Environment) StreamExists(streamName string) (bool, error) {
	err := env.maybeReconnectLocator()
	if err != nil {
		return false, err
	}
	return env.locator.client.StreamExists(streamName), nil
}

func (env *Environment) QueryOffset(consumerName string, streamName string) (int64, error) {
	err := env.maybeReconnectLocator()
	if err != nil {
		return 0, err
	}
	return env.locator.client.queryOffset(consumerName, streamName)
}

// QuerySequence gets the last id stored for a producer
// you can also see producer.GetLastPublishingId() that is the easier way to get the last-id
func (env *Environment) QuerySequence(publisherReference string, streamName string) (int64, error) {
	err := env.maybeReconnectLocator()
	if err != nil {
		return 0, err
	}
	return env.locator.client.queryPublisherSequence(publisherReference, streamName)
}

func (env *Environment) StreamStats(streamName string) (*StreamStats, error) {
	err := env.maybeReconnectLocator()
	if err != nil {
		return nil, err
	}
	return env.locator.client.StreamStats(streamName)
}

func (env *Environment) StreamMetaData(streamName string) (*StreamMetadata, error) {
	err := env.maybeReconnectLocator()
	if err != nil {
		return nil, err
	}
	streamsMetadata := env.locator.client.metaData(streamName)
	if streamsMetadata == nil {
		return nil, StreamMetadataFailure
	}
	streamMetadata := streamsMetadata.Get(streamName)
	if streamMetadata.responseCode != responseCodeOk {
		return nil, lookErrorCode(streamMetadata.responseCode)
	}

	tentatives := 0
	for streamMetadata == nil || streamMetadata.Leader == nil && tentatives < 3 {
		streamsMetadata = env.locator.client.metaData(streamName)
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
	err := env.maybeReconnectLocator()
	if err != nil {
		return nil, err
	}

	return env.consumers.NewSubscriber(env.locator.client, streamName, messagesHandler, options, env.options.AddressResolver, env.options.RPCTimeout)
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
	if env.locator.client != nil {
		env.locator.client.Close()
	}
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

type clientOptions interface {
	GetClientProvidedName(defaultClientProvidedName string) string
}

type environmentCoordinator struct {
	mutex             *sync.Mutex
	clientsPerContext sync.Map
	maxItemsForClient int
	nextId            int
}

func (cc *environmentCoordinator) isProducerListFull(clientsPerContextId int) bool {
	client, ok := cc.clientsPerContext.Load(clientsPerContextId)
	if !ok {
		logs.LogError("client not found")
		return false
	}
	return client.(*Client).coordinator.ProducersCount() >= cc.maxItemsForClient
}

func (cc *environmentCoordinator) isConsumerListFull(clientsPerContextId int) bool {
	client, ok := cc.clientsPerContext.Load(clientsPerContextId)
	if !ok {
		logs.LogError("client not found")
		return false
	}
	return client.(*Client).coordinator.ConsumersCount() >= cc.maxItemsForClient
}

func (cc *environmentCoordinator) maybeCleanClients() {
	cc.mutex.Lock()
	defer cc.mutex.Unlock()

	cc.clientsPerContext.Range(func(key, value any) bool {
		client := value.(*Client)
		if !client.socket.isOpen() {
			cc.clientsPerContext.Delete(key)
		}
		return true
	})
}

func (c *Client) maybeCleanProducers(streamName string) {
	c.coordinator.Producers().Range(func(pidx, p any) bool {
		producer := p.(*Producer)
		if producer.GetStreamName() == streamName {
			err := c.coordinator.RemoveProducerById(pidx.(uint8), Event{
				Command:    CommandMetadataUpdate,
				StreamName: streamName,
				Name:       producer.GetName(),
				Reason:     MetaDataUpdate,
				Err:        nil,
			})
			if err != nil {
				return false
			}
		}

		return true
	})
}

func (c *Client) maybeCleanConsumers(streamName string) {
	c.coordinator.Consumers().Range(func(pidx, cs any) bool {
		consumer := cs.(*Consumer)
		if consumer.options.streamName == streamName {
			err := c.coordinator.RemoveConsumerById(pidx.(uint8), Event{
				Command:    CommandMetadataUpdate,
				StreamName: streamName,
				Name:       consumer.GetName(),
				Reason:     MetaDataUpdate,
				Err:        nil,
			})
			if err != nil {
				return false
			}
		}

		return true
	})
}

func (cc *environmentCoordinator) newClientEntity(
	isListFull func(int) bool,
	defaultClientName string,
	leader *Broker,
	tcpParameters *TCPParameters,
	saslConfiguration *SaslConfiguration,
	options clientOptions,
	rpcTimeout time.Duration,
) (*Client, error) {
	cc.mutex.Lock()
	defer cc.mutex.Unlock()
	var clientResult *Client

	cc.clientsPerContext.Range(func(key, value any) bool {
		if !isListFull(key.(int)) {
			clientResult = value.(*Client)
			return false
		}
		return true
	})

	clientProvidedName := defaultClientName
	if options != nil {
		clientProvidedName = options.GetClientProvidedName(defaultClientName)
	}

	if clientResult == nil {
		clientResult = cc.newClientForConnection(clientProvidedName, leader, tcpParameters, saslConfiguration, rpcTimeout)
	}

	err := clientResult.connect()
	if err != nil {
		return nil, err
	}

	return cc.validateBrokerConnection(clientResult, leader,
		func() *Client {
			return cc.newClientForConnection(clientProvidedName, leader, tcpParameters, saslConfiguration, rpcTimeout)
		})
}

func (cc *environmentCoordinator) newProducer(leader *Broker, tcpParameters *TCPParameters, saslConfiguration *SaslConfiguration, streamName string, options *ProducerOptions, rpcTimeout time.Duration, cleanUp func()) (*Producer, error) {
	client, err := cc.newClientEntity(cc.isProducerListFull, "go-stream-producer", leader, tcpParameters, saslConfiguration, options, rpcTimeout)
	if err != nil {
		return nil, err
	}
	return client.declarePublisher(streamName, options, cleanUp)
}

func (cc *environmentCoordinator) newConsumer(leader *Broker, tcpParameters *TCPParameters, saslConfiguration *SaslConfiguration,
	streamName string, messagesHandler MessagesHandler,
	options *ConsumerOptions, rpcTimeout time.Duration, cleanUp func()) (*Consumer, error) {
	client, err := cc.newClientEntity(cc.isConsumerListFull, "go-stream-consumer", leader, tcpParameters, saslConfiguration, options, rpcTimeout)
	if err != nil {
		return nil, err
	}

	return client.declareSubscriber(streamName, messagesHandler, options, cleanUp)
}

func (cc *environmentCoordinator) validateBrokerConnection(client *Client, broker *Broker, newClientFunc func() *Client) (*Client, error) {
	for client.connectionProperties.host != broker.advHost ||
		client.connectionProperties.port != broker.advPort {
		logs.LogDebug("connectionProperties host %s doesn't match with the advertised_host %s, advertised_port %s .. retry",
			client.connectionProperties.host,
			broker.advHost, broker.advPort)
		client.Close()
		client = newClientFunc()
		err := client.connect()
		if err != nil {
			return nil, err
		}
		time.Sleep(time.Duration(500+rand.Intn(1000)) * time.Millisecond)
	}
	return client, nil
}

func (cc *environmentCoordinator) newClientForConnection(connectionName string, broker *Broker, tcpParameters *TCPParameters, saslConfiguration *SaslConfiguration, rpcTimeout time.Duration) *Client {
	clientResult := newClient(connectionParameters{
		connectionName:    connectionName,
		broker:            broker,
		tcpParameters:     tcpParameters,
		saslConfiguration: saslConfiguration,
		rpcTimeOut:        rpcTimeout,
	})
	cc.nextId++
	cc.clientsPerContext.Store(cc.nextId, clientResult)
	return clientResult
}

func (cc *environmentCoordinator) Close() error {
	cc.clientsPerContext.Range(func(_, value any) bool {
		value.(*Client).coordinator.Close()

		return true
	})

	return nil
}

func (cc *environmentCoordinator) getClientsPerContext() map[int]*Client {
	clients := map[int]*Client{}
	cc.clientsPerContext.Range(func(key, value any) bool {
		clients[key.(int)] = value.(*Client)
		return true
	})
	return clients
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

	leader, err := clientLocator.BrokerLeaderWithResolver(streamName, resolver)
	if err != nil {
		return nil, err
	}

	coordinatorKey := leader.hostPort()
	if ps.producersCoordinator[coordinatorKey] == nil {
		ps.producersCoordinator[coordinatorKey] = &environmentCoordinator{
			clientsPerContext: sync.Map{},
			mutex:             &sync.Mutex{},
			maxItemsForClient: ps.maxItemsForClient,
			nextId:            0,
		}
	}

	leader.cloneFrom(clientLocator.broker, resolver)

	cleanUp := func() {
		for _, coordinator := range ps.producersCoordinator {
			coordinator.maybeCleanClients()
		}
	}

	producer, err := ps.producersCoordinator[coordinatorKey].newProducer(leader, clientLocator.tcpParameters,
		clientLocator.saslConfiguration, streamName, options, rpcTimeOut, cleanUp)
	if err != nil {
		return nil, err
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
			clientsPerContext: sync.Map{},
			mutex:             &sync.Mutex{},
			maxItemsForClient: ps.maxItemsForClient,
			nextId:            0,
		}
	}

	consumerBroker.cloneFrom(clientLocator.broker, resolver)

	cleanUp := func() {
		for _, coordinator := range ps.consumersCoordinator {
			coordinator.maybeCleanClients()
		}
	}

	consumer, err := ps.consumersCoordinator[coordinatorKey].
		newConsumer(consumerBroker, clientLocator.tcpParameters,
			clientLocator.saslConfiguration,
			streamName, messagesHandler, consumerOptions, rpcTimeout, cleanUp)
	if err != nil {
		return nil, err
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
	err := env.maybeReconnectLocator()
	if err != nil {
		return err
	}
	if err := env.locator.client.DeclareSuperStream(superStreamName, options); err != nil && !errors.Is(err, StreamAlreadyExists) {
		return err
	}
	return nil
}

func (env *Environment) DeleteSuperStream(superStreamName string) error {
	err := env.maybeReconnectLocator()
	if err != nil {
		return err
	}
	return env.locator.client.DeleteSuperStream(superStreamName)
}

func (env *Environment) QueryPartitions(superStreamName string) ([]string, error) {
	err := env.maybeReconnectLocator()
	if err != nil {
		return nil, err
	}
	return env.locator.client.QueryPartitions(superStreamName)
}

// StoreOffset stores the offset for a consumer for a stream
// You should use the StoreOffset method of the Consumer interface
// to store the offset for a consumer
// The StoreOffset should not be called for each message.
// the best practice is to store after a batch of messages
// StoreOffset does not return any application error, if the stream does not exist or the consumer does not exist
// the error is logged in the server
func (env *Environment) StoreOffset(consumerName string, streamName string, offset int64) error {
	err := env.maybeReconnectLocator()
	if err != nil {
		return err
	}
	return env.locator.client.StoreOffset(consumerName, streamName, offset)
}

func (env *Environment) QueryRoute(superStream string, routingKey string) ([]string, error) {
	err := env.maybeReconnectLocator()
	if err != nil {
		return nil, err
	}
	return env.locator.client.queryRoute(superStream, routingKey)
}

func (env *Environment) NewSuperStreamConsumer(superStream string, messagesHandler MessagesHandler, options *SuperStreamConsumerOptions) (*SuperStreamConsumer, error) {
	s, err := newSuperStreamConsumer(env, superStream, messagesHandler, options)
	if err != nil {
		return nil, err
	}
	err = s.init()
	return s, err
}
