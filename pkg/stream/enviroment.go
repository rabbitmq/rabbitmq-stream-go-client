package stream

import (
	"crypto/tls"
	"errors"
	"math/rand"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/logs"
)

type Environment struct {
	consumers *consumersEnvironment
	options   *EnvironmentOptions
}

func NewEnvironment(options *EnvironmentOptions) (*Environment, error) {
	if options == nil {
		options = NewEnvironmentOptions()
	}
	client := newClient("go-stream-locator", nil, options.TCPParameters)
	defer func(client *Client) {
		err := client.Close()
		if err != nil {
			return
		}
	}(client)

	if len(options.ConnectionParameters) == 0 {
		options.ConnectionParameters = []*Broker{newBrokerDefault()}
	}

	for _, parameter := range options.ConnectionParameters {

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
	}
	return &Environment{
		options:   options,
		consumers: newConsumerEnvironment(options.MaxConsumersPerClient),
	}, client.connect()
}
func (env *Environment) newReconnectClient() (*Client, error) {
	broker := env.options.ConnectionParameters[0]
	client := newClient("go-stream-locator", broker, env.options.TCPParameters)

	err := client.connect()
	tentatives := 1
	for err != nil {
		logs.LogError("Can't connect the locator client, error:%s, retry in %d seconds, broker: ", err, tentatives,
			client.broker)
		time.Sleep(time.Duration(tentatives) * time.Second)
		rand.Seed(time.Now().UnixNano())
		n := rand.Intn(len(env.options.ConnectionParameters))
		client = newClient("stream-locator", env.options.ConnectionParameters[n], env.options.TCPParameters)
		tentatives = tentatives + 1
		err = client.connect()
		//TODO: Limit the retry
	}

	return client, client.connect()
}

func (env *Environment) DeclareStream(streamName string, options *StreamOptions) error {
	client, err := env.newReconnectClient()
	defer func(client *Client) {
		err := client.Close()
		if err != nil {
			return
		}
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
	client, err := env.newReconnectClient()
	defer func(client *Client) {
		err := client.Close()
		if err != nil {
			return
		}
	}(client)
	if err != nil {
		return nil, err
	}

	return env.newProducer(client, streamName, producerOptions, env.options.AddressResolver)
}

func (env *Environment) StreamExists(streamName string) (bool, error) {
	client, err := env.newReconnectClient()
	defer func(client *Client) {
		err := client.Close()
		if err != nil {
			return
		}
	}(client)
	if err != nil {
		return false, err
	}
	return client.StreamExists(streamName), nil
}

func (env *Environment) QueryOffset(consumerName string, streamName string) (int64, error) {
	client, err := env.newReconnectClient()
	defer func(client *Client) {
		err := client.Close()
		if err != nil {
			return
		}
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
		err := client.Close()
		if err != nil {
			return
		}
	}(client)
	if err != nil {
		return 0, err
	}
	return client.queryPublisherSequence(publisherReference, streamName)
}

func (env *Environment) StreamMetaData(streamName string) (*StreamMetadata, error) {
	client, err := env.newReconnectClient()
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
		err := client.Close()
		if err != nil {
			return
		}
	}(client)
	if err != nil {
		return nil, err
	}

	return env.consumers.NewSubscriber(client, streamName, messagesHandler, options, env.options.AddressResolver)
}

func (env *Environment) Close() error {
	_ = env.consumers.close()
	return nil
}

type EnvironmentOptions struct {
	ConnectionParameters  []*Broker
	TCPParameters         *TCPParameters
	MaxConsumersPerClient int
	AddressResolver       *AddressResolver
}

func NewEnvironmentOptions() *EnvironmentOptions {
	return &EnvironmentOptions{
		MaxConsumersPerClient: 1,
		ConnectionParameters:  []*Broker{},
		TCPParameters:         newTCPParameterDefault(),
	}
}

func (envOptions *EnvironmentOptions) SetAddressResolver(addressResolver AddressResolver) *EnvironmentOptions {
	envOptions.AddressResolver = &AddressResolver{
		Host: addressResolver.Host,
		Port: addressResolver.Port,
	}
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

type environmentCoordinator struct {
	mutex             *sync.Mutex
	mutexContext      *sync.RWMutex
	clientsPerContext map[int]*Client
	maxItemsForClient int
	nextId            int
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

func (client *Client) maybeCleanConsumers(streamName string) {
	client.mutex.Lock()
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
	client.mutex.Unlock()
	if client.coordinator.ConsumersCount() == 0 {
		err := client.Close()
		if err != nil {
			return
		}
	}
}

func (cc *environmentCoordinator) newConsumer(leader *Broker, tcpParameters *TCPParameters,
	streamName string, messagesHandler MessagesHandler,
	options *ConsumerOptions) (*Consumer, error) {
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
		clientResult = newClient("go-stream-consumer", leader, tcpParameters)
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

func (cc *environmentCoordinator) close() error {
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

func (ps *Environment) newProducer(clientLocator *Client, streamName string,
	options *ProducerOptions, resolver *AddressResolver) (*Producer, error) {

	leader, err := clientLocator.BrokerLeader(streamName)
	if err != nil {
		return nil, err
	}

	leader.setBrokerProperties(clientLocator.broker, resolver)
	clientLeader := newClient("go-stream-producer", leader, clientLocator.tcpParameters)

	err = clientLeader.connect()
	if err != nil {
		return nil, err
	}

	return clientLeader.DeclarePublisher(streamName, options)
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
	consumerOptions *ConsumerOptions, resolver *AddressResolver) (*Consumer, error) {
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
	consumerBroker.setBrokerProperties(clientLocator.broker, resolver)
	consumer, err := ps.consumersCoordinator[coordinatorKey].
		newConsumer(consumerBroker, clientLocator.tcpParameters, streamName, messagesHandler, consumerOptions)
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

func (ps *consumersEnvironment) getCoordinators() map[string]*environmentCoordinator {
	ps.mutex.Lock()
	defer ps.mutex.Unlock()
	return ps.consumersCoordinator
}
