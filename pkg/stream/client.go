package stream

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/logs"
)

// SaslConfiguration see
//
//	SaslConfigurationPlain    = "PLAIN"
//	SaslConfigurationExternal = "EXTERNAL"
//

type SaslConfiguration struct {
	Mechanism string
}

func newSaslConfigurationDefault() *SaslConfiguration {
	return &SaslConfiguration{
		Mechanism: SaslConfigurationPlain,
	}
}

type TuneState struct {
	requestedMaxFrameSize int
	requestedHeartbeat    int
}

type ClientProperties struct {
	items map[string]string
}

type ConnectionProperties struct {
	host string
	port string
}

type HeartBeat struct {
	mutex sync.Mutex
	value time.Time
}

type Client struct {
	socket               socket
	destructor           *sync.Once
	clientProperties     ClientProperties
	connectionProperties ConnectionProperties
	tuneState            TuneState
	coordinator          *Coordinator
	broker               *Broker
	tcpParameters        *TCPParameters
	saslConfiguration    *SaslConfiguration

	mutex             *sync.Mutex
	lastHeartBeat     HeartBeat
	socketCallTimeout time.Duration
	availableFeatures *availableFeatures
	serverProperties  map[string]string

	doneTimeoutTicker chan struct{}
	uniqueId          string
}

func newClient(parameters clientConnectionParameters) *Client {
	var clientBroker = parameters.broker
	if parameters.broker == nil {
		clientBroker = newBrokerDefault()
	}
	if parameters.tcpParameters == nil {
		parameters.tcpParameters = newTCPParameterDefault()
	}

	if parameters.saslConfiguration == nil {
		parameters.saslConfiguration = newSaslConfigurationDefault()
	}

	if parameters.rpcTimeOut == 0 {
		parameters.rpcTimeOut = defaultConfirmationTimeOut
	}

	c := &Client{
		coordinator:          NewCoordinator(),
		broker:               clientBroker,
		tcpParameters:        parameters.tcpParameters,
		saslConfiguration:    parameters.saslConfiguration,
		destructor:           &sync.Once{},
		mutex:                &sync.Mutex{},
		clientProperties:     ClientProperties{items: make(map[string]string)},
		connectionProperties: ConnectionProperties{},
		lastHeartBeat: HeartBeat{
			value: time.Now(),
		},
		socket: socket{
			mutex:      &sync.Mutex{},
			destructor: &sync.Once{},
		},
		socketCallTimeout: parameters.rpcTimeOut,
		availableFeatures: newAvailableFeatures(),
		doneTimeoutTicker: make(chan struct{}, 1),
		uniqueId:          uuid.New().String(),
	}
	c.setConnectionName(parameters.connectionName)
	return c
}

func (c *Client) getSocket() *socket {
	return &c.socket
}

func (c *Client) setSocketConnection(connection net.Conn) {
	c.socket.connection = connection
	c.socket.writer = bufio.NewWriter(connection)
}

func (c *Client) getTuneState() TuneState {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.tuneState
}

func (c *Client) getLastHeartBeat() time.Time {
	c.lastHeartBeat.mutex.Lock()
	defer c.lastHeartBeat.mutex.Unlock()
	return c.lastHeartBeat.value
}

func (c *Client) setLastHeartBeat(value time.Time) {
	c.lastHeartBeat.mutex.Lock()
	defer c.lastHeartBeat.mutex.Unlock()
	c.lastHeartBeat.value = value
}

func (c *Client) connect() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if !c.socket.isOpen() {
		u, err := url.Parse(c.broker.GetUri())
		if err != nil {
			return err
		}
		host, port := u.Hostname(), u.Port()
		c.tuneState.requestedMaxFrameSize = c.tcpParameters.RequestedMaxFrameSize
		c.tuneState.requestedHeartbeat = int(c.tcpParameters.RequestedHeartbeat.Seconds())

		servAddr := net.JoinHostPort(host, port)
		tcpAddr, errorResolve := net.ResolveTCPAddr("tcp", servAddr)
		if errorResolve != nil {
			logs.LogDebug("Resolve error %s", errorResolve)
			return errorResolve
		}
		connection, errorConnection := net.DialTCP("tcp", nil, tcpAddr)
		if errorConnection != nil {
			logs.LogDebug("%s", errorConnection)
			return errorConnection
		}

		if c.tcpParameters.WriteBuffer > 0 {
			if err = connection.SetWriteBuffer(c.tcpParameters.WriteBuffer); err != nil {
				logs.LogError("Failed to SetWriteBuffer to %d due to %v", c.tcpParameters.WriteBuffer, err)
				return err
			}
		}

		if c.tcpParameters.ReadBuffer > 0 {
			if err = connection.SetReadBuffer(c.tcpParameters.ReadBuffer); err != nil {
				logs.LogError("Failed to SetReadBuffer to %d due to %v", c.tcpParameters.ReadBuffer, err)
				return err
			}
		}

		if err = connection.SetNoDelay(c.tcpParameters.NoDelay); err != nil {
			logs.LogError("Failed to SetNoDelay to %b due to %v", c.tcpParameters.NoDelay, err)
			return err
		}

		if c.broker.isTLS() {
			//nolint:gosec
			conf := &tls.Config{}
			if c.tcpParameters.tlsConfig != nil {
				conf = c.tcpParameters.tlsConfig
			}
			c.setSocketConnection(tls.Client(connection, conf))
		} else {
			c.setSocketConnection(connection)
		}

		c.socket.setOpen()

		go c.handleResponse()
		serverProperties, err2 := c.peerProperties()
		c.serverProperties = serverProperties
		if err2 != nil {
			logs.LogError("Can't set the peer-properties. Check if the stream server is running/reachable")
			return err2
		}

		pwd, _ := u.User.Password()
		err2 = c.authenticate(u.User.Username(), pwd)
		if err2 != nil {
			logs.LogDebug("User:%s, %s", u.User.Username(), err2)
			return err2
		}
		vhost := "/"
		if len(u.Path) > 1 {
			vhost, _ = url.QueryUnescape(u.Path[1:])
		}
		err2 = c.open(vhost)
		if err2 != nil {
			logs.LogDebug("%s", err2)
			return err2
		}

		err = c.availableFeatures.SetVersion(serverProperties["version"])
		if err != nil {
			logs.LogWarn("Error checking server version: %s", err)
		}

		if serverProperties["version"] == "" || !c.availableFeatures.Is311OrMore() {
			logs.LogDebug(
				"Server version is less than 3.11.0, skipping command version exchange")
		} else {
			err := c.exchangeVersion(c.serverProperties["version"])
			if err != nil {
				return err
			}
			logs.LogDebug("available features: %s", c.availableFeatures)
		}

		c.heartBeat()
		logs.LogDebug("User %s, connected to: %s, vhost:%s", u.User.Username(),
			net.JoinHostPort(host, port),
			vhost)
	}
	return nil
}

func (c *Client) setConnectionName(connectionName string) {
	c.clientProperties.items["connection_name"] = connectionName
}

func (c *Client) peerProperties() (map[string]string, error) {
	clientPropertiesSize := 4 // size of the map, always there

	c.clientProperties.items["product"] = "RabbitMQ Stream"
	c.clientProperties.items["copyright"] = "Copyright (c) 2021 VMware, Inc. or its affiliates."
	c.clientProperties.items["information"] = "Licensed under the MPL 2.0. See https://www.rabbitmq.com/"
	c.clientProperties.items["version"] = ClientVersion
	c.clientProperties.items["platform"] = "Golang"
	for key, element := range c.clientProperties.items {
		clientPropertiesSize = clientPropertiesSize + 2 + len(key) + 2 + len(element)
	}

	length := 2 + 2 + 4 + clientPropertiesSize
	resp := c.coordinator.NewResponse(commandPeerProperties)
	correlationId := resp.correlationid
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	writeProtocolHeader(b, length, commandPeerProperties,
		correlationId)
	writeInt(b, len(c.clientProperties.items))

	for key, element := range c.clientProperties.items {
		writeString(b, key)
		writeString(b, element)
	}

	err := c.handleWriteWithResponse(b.Bytes(), resp, false)
	if err.Err != nil {
		return nil, err.Err
	}

	serverProperties := <-resp.data
	_ = c.coordinator.RemoveResponseById(resp.correlationid)
	return serverProperties.(map[string]string), nil
}

func (c *Client) authenticate(user string, password string) error {
	saslMechanisms, err := c.getSaslMechanisms()
	if err != nil {
		return err
	}
	saslMechanism := ""
	for i := range saslMechanisms {
		if saslMechanisms[i] == c.saslConfiguration.Mechanism {
			saslMechanism = c.saslConfiguration.Mechanism
			break
		}
	}
	if saslMechanism == "" {
		return fmt.Errorf("no matching mechanism found")
	}

	response := unicodeNull + user + unicodeNull + password
	saslResponse := []byte(response)
	return c.sendSaslAuthenticate(saslMechanism, saslResponse)
}

func (c *Client) getSaslMechanisms() ([]string, error) {
	length := 2 + 2 + 4
	resp := c.coordinator.NewResponse(commandSaslHandshake)
	correlationId := resp.correlationid
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	writeProtocolHeader(b, length, commandSaslHandshake,
		correlationId)

	errWrite := c.socket.writeAndFlush(b.Bytes())
	data := <-resp.data
	err := c.coordinator.RemoveResponseById(correlationId)
	if err != nil {
		return nil, err
	}
	if errWrite != nil {
		return nil, errWrite
	}
	return data.([]string), nil
}

func (c *Client) sendSaslAuthenticate(saslMechanism string, challengeResponse []byte) error {
	length := 2 + 2 + 4 + 2 + len(saslMechanism) + 4 + len(challengeResponse)
	resp := c.coordinator.NewResponse(commandSaslAuthenticate)
	respTune := c.coordinator.NewResponseWitName("tune")
	correlationId := resp.correlationid
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	writeProtocolHeader(b, length, commandSaslAuthenticate,
		correlationId)

	writeString(b, saslMechanism)
	writeInt(b, len(challengeResponse))
	b.Write(challengeResponse)
	err := c.handleWrite(b.Bytes(), resp)
	if err.Err != nil {
		return err.Err
	}
	// double read for TUNE
	tuneData := <-respTune.data
	errR := c.coordinator.RemoveResponseByName("tune")
	if errR != nil {
		return errR
	}

	return c.socket.writeAndFlush(tuneData.([]byte))
}

func (c *Client) exchangeVersion(serverVersion string) error {
	_ = c.availableFeatures.SetVersion(serverVersion)

	commands := c.availableFeatures.GetCommands()

	length := 2 + 2 + 4 +
		4 + // commands size
		len(commands)*(2+2+2)
	resp := c.coordinator.NewResponse(commandExchangeVersion)
	correlationId := resp.correlationid
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	writeProtocolHeader(b, length, commandExchangeVersion,
		correlationId)

	writeInt(b, len(commands))

	for _, command := range commands {
		writeUShort(b, command.GetCommandKey())
		writeUShort(b, command.GetMinVersion())
		writeUShort(b, command.GetMaxVersion())
	}

	err := c.handleWriteWithResponse(b.Bytes(), resp, false)
	if err.Err != nil {
		return err.Err
	}

	commandsResponse := <-resp.data
	_ = c.coordinator.RemoveResponseById(resp.correlationid)
	c.availableFeatures.ParseCommandVersions(commandsResponse.([]commandVersion))
	return nil
}

func (c *Client) open(virtualHost string) error {
	length := 2 + 2 + 4 + 2 + len(virtualHost)
	resp := c.coordinator.NewResponse(commandOpen, virtualHost)
	correlationId := resp.correlationid
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	writeProtocolHeader(b, length, commandOpen,
		correlationId)
	writeString(b, virtualHost)
	err := c.handleWriteWithResponse(b.Bytes(), resp, false)
	if err.Err != nil {
		return err.Err
	}

	advHostPort := <-resp.data
	c.connectionProperties.host = advHostPort.(ConnectionProperties).host
	c.connectionProperties.port = advHostPort.(ConnectionProperties).port

	_ = c.coordinator.RemoveResponseById(resp.correlationid)
	return nil
}

func (c *Client) DeleteStream(streamName string) error {
	length := 2 + 2 + 4 + 2 + len(streamName)
	resp := c.coordinator.NewResponse(commandDeleteStream, streamName)
	correlationId := resp.correlationid
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	writeProtocolHeader(b, length, commandDeleteStream,
		correlationId)

	writeString(b, streamName)

	return c.handleWrite(b.Bytes(), resp).Err
}

func (c *Client) heartBeat() {
	tickerHeartbeat := time.NewTicker(time.Duration(c.tuneState.requestedHeartbeat) * time.Second)

	var heartBeatMissed int32

	go func() {
		for {
			select {
			case <-c.doneTimeoutTicker:
				tickerHeartbeat.Stop()
				return
			case <-tickerHeartbeat.C:
				if c.socket.isOpen() {
					logs.LogDebug("Heartbeat ticker is open, sending heartbeat")
					c.sendHeartbeat()
					if time.Since(c.getLastHeartBeat()) > time.Duration(c.tuneState.requestedHeartbeat)*time.Second {
						v := atomic.AddInt32(&heartBeatMissed, 1)
						logs.LogWarn("Missing heart beat: %d", v)
						if v >= 2 {
							logs.LogWarn("Too many heartbeat missing: %d", v)
							c.Close()
						}
					} else {
						atomic.StoreInt32(&heartBeatMissed, 0)
					}
				} else {
					logs.LogDebug("Socket Heartbeat ticker is closed. Closing ticker")
					tickerHeartbeat.Stop()
					return
				}
			}
		}
	}()
}

func (c *Client) sendHeartbeat() {
	length := 4
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	writeProtocolHeader(b, length, commandHeartbeat)
	_ = c.socket.writeAndFlush(b.Bytes())
}

func (c *Client) closeHartBeat() {
	c.destructor.Do(func() {
		c.doneTimeoutTicker <- struct{}{}
		close(c.doneTimeoutTicker)
	})
}

// *** interface IClient implementation ***

// Close the client and all the associated producers and consumers
func (c *Client) Close() {
	c.closeHartBeat()
	c.coordinator.Producers().Range(func(_, p any) bool {
		producer := p.(*Producer)
		err := c.coordinator.RemoveProducerById(producer.id, Event{
			Command:    CommandClose,
			StreamName: producer.GetStreamName(),
			Name:       producer.GetName(),
			Reason:     SocketClosed,
			Err:        nil,
		})

		if err != nil {
			logs.LogWarn("error removing producer: %s", err)
		}

		return true
	})

	c.coordinator.Consumers().Range(func(_, cs any) bool {
		consumer := cs.(*Consumer)
		err := c.coordinator.RemoveConsumerById(consumer.id, Event{
			Command:    CommandClose,
			StreamName: consumer.GetStreamName(),
			Name:       consumer.GetName(),
			Reason:     SocketClosed,
			Err:        nil,
		})

		if err != nil {
			logs.LogWarn("error removing consumer: %s", err)
		}

		return true
	})

	if c.getSocket().isOpen() {
		res := c.coordinator.NewResponse(CommandClose)
		length := 2 + 2 + 4 + 2 + 2 + len("OK")
		var b = bytes.NewBuffer(make([]byte, 0, length+4))
		writeProtocolHeader(b, length, CommandClose, res.correlationid)
		writeUShort(b, responseCodeOk)
		writeString(b, "OK")

		errW := c.socket.writeAndFlush(b.Bytes())
		if errW != nil {
			logs.LogWarn("error during Send client close %s", errW)
		}
		_ = c.coordinator.RemoveResponseById(res.correlationid)
	}
	c.getSocket().shutdown(nil)
}

func (c *Client) Entities() []IEntity {
	return nil
}

func (c *Client) RemoveEntityById(_ uint8) {
	// no-op
}

func (c *Client) AddEntity(_ IEntity) {
	// no-op
}

func (c *Client) GetUniqueId() string {
	return c.uniqueId
}

// **** end of interface IClient implementation ****

func (c *Client) DeclarePublisher(streamName string, options *ProducerOptions) (*Producer, error) {
	return c.declarePublisher(streamName, options, nil)
}

func (c *Client) declarePublisher(streamName string, options *ProducerOptions, cleanUp func()) (*Producer, error) {
	if options == nil {
		options = NewProducerOptions()
	}

	if options.IsFilterEnabled() && !c.availableFeatures.BrokerFilterEnabled() {
		return nil, FilterNotSupported
	}

	if options.isSubEntriesBatching() && options.IsFilterEnabled() {
		return nil, fmt.Errorf("sub-entry batching can't be enabled with filter")
	}
	if options.QueueSize < minQueuePublisherSize || options.QueueSize > maxQueuePublisherSize {
		return nil, fmt.Errorf("QueueSize values must be between %d and %d",
			minQueuePublisherSize, maxQueuePublisherSize)
	}

	if options.BatchSize < minBatchSize || options.BatchSize > maxBatchSize {
		return nil, fmt.Errorf("BatchSize values must be between %d and %d",
			minBatchSize, maxBatchSize)
	}

	if options.BatchPublishingDelay < minBatchPublishingDelay || options.BatchPublishingDelay > maxBatchPublishingDelay {
		return nil, fmt.Errorf("BatchPublishingDelay values must be between %d and %d",
			minBatchPublishingDelay, maxBatchPublishingDelay)
	}

	if options.SubEntrySize < minSubEntrySize || options.SubEntrySize > maxSubEntrySize {
		return nil, fmt.Errorf("SubEntrySize values must be between %d and %d",
			minSubEntrySize, maxSubEntrySize)
	}

	if !options.isSubEntriesBatching() {
		if options.Compression.enabled {
			return nil, fmt.Errorf("sub-entry batching must be enabled to enable compression")
		}
	}

	if !options.isSubEntriesBatching() {
		if options.Compression.value != None && options.Compression.value != GZIP {
			return nil, fmt.Errorf("compression values valid are: %d (None) %d (Gzip)", None, GZIP)
		}
	}

	producer, err := c.coordinator.NewProducer(&ProducerOptions{
		streamName:           streamName,
		Name:                 options.Name,
		QueueSize:            options.QueueSize,
		BatchSize:            options.BatchSize,
		BatchPublishingDelay: options.BatchPublishingDelay,
		SubEntrySize:         options.SubEntrySize,
		Compression:          options.Compression,
		ConfirmationTimeOut:  options.ConfirmationTimeOut,
		ClientProvidedName:   options.ClientProvidedName,
		Filter:               options.Filter,
	}, cleanUp)

	if err != nil {
		return nil, err
	}
	producer.client = c
	res := c.internalDeclarePublisher(streamName, producer)
	if res.Err == nil {
		producer.startUnconfirmedMessagesTimeOutTask()
		producer.processPendingSequencesQueue()
	}
	return producer, res.Err
}

func (c *Client) internalDeclarePublisher(streamName string, producer *Producer) responseError {
	publisherReferenceSize := 0
	if producer.options != nil {
		if producer.options.Name != "" {
			publisherReferenceSize = len(producer.options.Name)
		}
	}

	if publisherReferenceSize > 0 {
		v, err := c.queryPublisherSequence(producer.options.Name, streamName)
		if err != nil {
			// if the client can't get the sequence, the function will return an error
			// because is not able to set the sequence
			// in most of the case the error timeout is during the re-connection
			// in this case the producer can't be created and the client will return an error
			return responseError{Err: err}
		}
		producer.sequence = v
	}

	length := 2 + 2 + 4 + 1 + 2 + publisherReferenceSize + 2 + len(streamName)
	resp := c.coordinator.NewResponse(commandDeclarePublisher, streamName)
	correlationId := resp.correlationid
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	writeProtocolHeader(b, length, commandDeclarePublisher,
		correlationId)

	writeByte(b, producer.id)
	writeShort(b, int16(publisherReferenceSize))
	if publisherReferenceSize > 0 {
		writeBytes(b, []byte(producer.options.Name))
	}

	writeString(b, streamName)
	res := c.handleWrite(b.Bytes(), resp)

	return res
}

func (c *Client) metaData(streams ...string) *StreamsMetadata {
	length := 2 + 2 + 4 + 4 // API code, version, correlation id, size of array
	for _, stream := range streams {
		length += 2
		length += len(stream)
	}
	resp := c.coordinator.NewResponse(commandMetadata)
	correlationId := resp.correlationid
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	writeProtocolHeader(b, length, commandMetadata,
		correlationId)

	writeInt(b, len(streams))
	for _, stream := range streams {
		writeString(b, stream)
	}

	err := c.handleWriteWithResponse(b.Bytes(), resp, false)
	if err.Err != nil {
		return nil
	}

	data := <-resp.data
	_ = c.coordinator.RemoveResponseById(resp.correlationid)
	return data.(*StreamsMetadata)
}

func (c *Client) queryPublisherSequence(publisherReference string, stream string) (int64, error) {
	length := 2 + 2 + 4 + 2 + len(publisherReference) + 2 + len(stream)
	resp := c.coordinator.NewResponse(commandQueryPublisherSequence)
	correlationId := resp.correlationid
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	writeProtocolHeader(b, length, commandQueryPublisherSequence, correlationId)

	writeString(b, publisherReference)
	writeString(b, stream)
	err := c.handleWriteWithResponse(b.Bytes(), resp, false)
	if err.Err != nil {
		return 0, err.Err
	}

	sequence := <-resp.data
	_ = c.coordinator.RemoveResponseById(resp.correlationid)
	return sequence.(int64), nil
}

func (c *Client) BrokerLeader(stream string) (*Broker, error) {
	return c.BrokerLeaderWithResolver(stream, nil)
}

func (c *Client) BrokerLeaderWithResolver(stream string, resolver *AddressResolver) (*Broker, error) {
	streamsMetadata := c.metaData(stream)
	if streamsMetadata == nil {
		return nil, fmt.Errorf("leader error for stream for stream: %s", stream)
	}

	streamMetadata := streamsMetadata.Get(stream)
	if streamMetadata.responseCode != responseCodeOk {
		return nil, lookErrorCode(streamMetadata.responseCode)
	}
	if streamMetadata.Leader == nil {
		return nil, LeaderNotReady
	}

	streamMetadata.Leader.advPort = streamMetadata.Leader.Port
	streamMetadata.Leader.advHost = streamMetadata.Leader.Host

	// If AddressResolver is configured, use it directly and skip DNS lookup
	if resolver != nil {
		streamMetadata.Leader.Host = resolver.Host
		streamMetadata.Leader.Port = strconv.Itoa(resolver.Port)
		return streamMetadata.Leader, nil
	}

	res := net.Resolver{}
	// see: https://github.com/rabbitmq/rabbitmq-stream-go-client/pull/317
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err := res.LookupIPAddr(ctx, streamMetadata.Leader.Host)
	if err != nil {
		var dnsError *net.DNSError
		if errors.As(err, &dnsError) {
			if strings.EqualFold(c.broker.Host, "localhost") {
				logs.LogWarn("Can't lookup the DNS for %s, error: %s. Trying localhost..", streamMetadata.Leader.Host, err)
				streamMetadata.Leader.Host = "localhost"
			} else {
				logs.LogWarn("Can't lookup the DNS for %s, error: %s", streamMetadata.Leader.Host, err)
			}
		}
	}

	return streamMetadata.Leader, nil
}

func (c *Client) StreamExists(stream string) bool {
	streamsMetadata := c.metaData(stream)
	if streamsMetadata == nil {
		return false
	}

	streamMetadata := streamsMetadata.Get(stream)
	return streamMetadata.responseCode == responseCodeOk
}
func (c *Client) BrokerForConsumer(stream string) (*Broker, error) {
	streamsMetadata := c.metaData(stream)
	if streamsMetadata == nil {
		return nil, fmt.Errorf("leader error for stream: %s", stream)
	}

	streamMetadata := streamsMetadata.Get(stream)
	if streamMetadata.responseCode != responseCodeOk {
		return nil, lookErrorCode(streamMetadata.responseCode)
	}

	if streamMetadata.Leader == nil {
		return nil, LeaderNotReady
	}

	brokers := make([]*Broker, 0, 1+len(streamMetadata.Replicas))

	// Count available replicas
	availableReplicas := 0
	for _, replica := range streamMetadata.Replicas {
		if replica != nil {
			availableReplicas++
		}
	}

	// Only add leader if no replicas are available
	if availableReplicas == 0 {
		streamMetadata.Leader.advPort = streamMetadata.Leader.Port
		streamMetadata.Leader.advHost = streamMetadata.Leader.Host
		brokers = append(brokers, streamMetadata.Leader)
	}

	// Add all available replicas
	for idx, replica := range streamMetadata.Replicas {
		if replica == nil {
			logs.LogWarn("Stream %s replica not ready: %d", stream, idx)
			continue
		}
		replica.advPort = replica.Port
		replica.advHost = replica.Host
		brokers = append(brokers, replica)
	}

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	n := r.Intn(len(brokers))
	return brokers[n], nil
}

func (c *Client) DeclareStream(streamName string, options *StreamOptions) error {
	if streamName == "" {
		return fmt.Errorf("stream Name can't be empty")
	}

	resp := c.coordinator.NewResponse(commandCreateStream, streamName)
	length := 2 + 2 + 4 + 2 + len(streamName) + 4
	correlationId := resp.correlationid
	if options == nil {
		options = NewStreamOptions()
	}

	args, err := options.buildParameters()
	if err != nil {
		_ = c.coordinator.RemoveResponseById(resp.correlationid)
		return err
	}
	for key, element := range args {
		length = length + 2 + len(key) + 2 + len(element)
	}
	var b = bytes.NewBuffer(make([]byte, 0, length))
	writeProtocolHeader(b, length, commandCreateStream,
		correlationId)
	writeString(b, streamName)
	writeInt(b, len(args))

	for key, element := range args {
		writeString(b, key)
		writeString(b, element)
	}

	return c.handleWrite(b.Bytes(), resp).Err
}

func (c *Client) queryOffset(consumerName string, streamName string) (int64, error) {
	length := 2 + 2 + 4 + 2 + len(consumerName) + 2 + len(streamName)

	resp := c.coordinator.NewResponse(CommandQueryOffset)
	correlationId := resp.correlationid
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	writeProtocolHeader(b, length, CommandQueryOffset,
		correlationId)

	writeString(b, consumerName)
	writeString(b, streamName)
	err := c.handleWriteWithResponse(b.Bytes(), resp, false)
	if err.Err != nil {
		return 0, err.Err
	}

	offset := <-resp.data
	_ = c.coordinator.RemoveResponseById(resp.correlationid)
	return offset.(int64), nil
}

func (c *Client) StoreOffset(consumerName string, streamName string, offset int64) error {
	length := 2 + 2 + 2 + len(consumerName) + 2 +
		len(streamName) + 8
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	writeProtocolHeader(b, length, commandStoreOffset)

	writeString(b, consumerName)
	writeString(b, streamName)

	writeLong(b, offset)
	return c.socket.writeAndFlush(b.Bytes())
}
func (c *Client) DeclareSubscriber(streamName string,
	messagesHandler MessagesHandler,
	options *ConsumerOptions) (*Consumer, error) {
	return c.declareSubscriber(streamName, messagesHandler, options, nil)
}

func (c *Client) declareSubscriber(streamName string,
	messagesHandler MessagesHandler,
	options *ConsumerOptions, cleanUp func()) (*Consumer, error) {
	if options == nil {
		options = NewConsumerOptions()
	}

	if options.initialCredits <= 0 {
		options.initialCredits = 10
	}
	if containsOnlySpaces(options.ConsumerName) {
		return nil, fmt.Errorf("consumer name contains only spaces")
	}

	if options.IsSingleActiveConsumerEnabled() && !c.availableFeatures.IsBrokerSingleActiveConsumerEnabled() {
		return nil, SingleActiveConsumerNotSupported
	}

	if options.IsSingleActiveConsumerEnabled() && strings.TrimSpace(options.ConsumerName) == "" {
		return nil, fmt.Errorf("single active enabled but name is empty. You need to set a name")
	}

	if options.IsSingleActiveConsumerEnabled() && options.SingleActiveConsumer.ConsumerUpdate == nil {
		return nil, fmt.Errorf("single active enabled but consumer update function  is nil. Consumer update must be set")
	}

	if options.IsFilterEnabled() && !c.availableFeatures.BrokerFilterEnabled() {
		return nil, FilterNotSupported
	}

	if options.IsFilterEnabled() && options.Filter.PostFilter == nil {
		return nil, fmt.Errorf("filter enabled but post filter is nil. Post filter must be set")
	}

	if options.IsFilterEnabled() && (len(options.Filter.Values) == 0) {
		return nil, fmt.Errorf("filter enabled but no values. At least one value must be set")
	}

	if options.IsFilterEnabled() {
		for _, value := range options.Filter.Values {
			if value == "" {
				return nil, fmt.Errorf("filter enabled but one of the value is empty")
			}
		}
	}

	if options.Offset.typeOfs <= 0 || options.Offset.typeOfs > 6 {
		return nil, fmt.Errorf("specify a valid Offset")
	}

	if (options.autoCommitStrategy != nil) && (options.autoCommitStrategy.flushInterval < 1*time.Second) && options.autocommit {
		return nil, fmt.Errorf("flush internal must be bigger than one second")
	}

	if (options.autoCommitStrategy != nil) && options.autoCommitStrategy.messageCountBeforeStorage < 1 && options.autocommit {
		return nil, fmt.Errorf("message count before storage must be bigger than one")
	}

	if (options.autoCommitStrategy != nil) && options.ConsumerName == "" && options.autocommit {
		return nil, fmt.Errorf("consumer name must be set when autocommit is enabled")
	}

	if messagesHandler == nil {
		return nil, fmt.Errorf("messages Handler must be set")
	}

	if options.Offset.isLastConsumed() {
		lastOffset, err := c.queryOffset(options.ConsumerName, streamName)
		switch {
		case err == nil, errors.Is(err, OffsetNotFoundError):
			if errors.Is(err, OffsetNotFoundError) {
				options.Offset.typeOfs = typeFirst
				options.Offset.offset = 0
				break
			} else {
				options.Offset.offset = lastOffset
				options.Offset.typeOfs = typeOffset
				break
			}
		default:
			return nil, err
		}
	}

	options.streamName = streamName
	consumer := c.coordinator.NewConsumer(messagesHandler, options, cleanUp)
	consumer.client = c
	length := 2 + 2 + 4 + 1 + 2 + len(streamName) + 2 + 2
	if options.Offset.isOffset() ||
		options.Offset.isTimestamp() {
		length += 8
	}

	// copy the option offset to the consumer offset
	// the option.offset won't change ( in case we need to retrieve the original configuration)
	// consumer.current offset will be moved when reading
	if !options.IsSingleActiveConsumerEnabled() && options.Offset.isOffset() {
		consumer.setCurrentOffset(options.Offset.offset)
	}

	/// define the consumerOptions
	consumerProperties := make(map[string]string)

	if options.ConsumerName != "" {
		consumerProperties["name"] = options.ConsumerName
	}

	if options.IsSingleActiveConsumerEnabled() {
		consumerProperties["single-active-consumer"] = "true"
		if options.SingleActiveConsumer.superStream != "" {
			consumerProperties["super-stream"] = options.SingleActiveConsumer.superStream
		}
	}

	if options.Filter != nil {
		for i, filterValue := range options.Filter.Values {
			k := fmt.Sprintf("%s%d", subscriptionPropertyFilterPrefix, i)
			consumerProperties[k] = filterValue
		}

		consumerProperties[subscriptionPropertyMatchUnfiltered] = strconv.FormatBool(options.Filter.MatchUnfiltered)
	}

	if len(consumerProperties) > 0 {
		length += 4 // size of the properties map

		for k, v := range consumerProperties {
			length += 2 + len(k)
			length += 2 + len(v)
		}
	}

	resp := c.coordinator.NewResponse(commandSubscribe, streamName)
	correlationId := resp.correlationid
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	writeProtocolHeader(b, length, commandSubscribe,
		correlationId)
	writeByte(b, consumer.id)

	writeString(b, streamName)

	writeShort(b, options.Offset.typeOfs)

	if options.Offset.isOffset() ||
		options.Offset.isTimestamp() {
		writeLong(b, options.Offset.offset)
	}
	writeShort(b, options.initialCredits)
	if len(consumerProperties) > 0 {
		writeInt(b, len(consumerProperties))
		for k, v := range consumerProperties {
			writeString(b, k)
			writeString(b, v)
		}
	}

	err := c.handleWrite(b.Bytes(), resp)

	canDispatch := func(offsetMessage *offsetMessage) bool {
		if !consumer.isActive() {
			logs.LogDebug("The consumer is not active anymore the message will be skipped, partition %s", streamName)
			return false
		}

		if options.IsFilterEnabled() && options.Filter.PostFilter != nil {
			return options.Filter.PostFilter(offsetMessage.message)
		}
		return true
	}
	go func() {
		for {
			select {
			case chunk, ok := <-consumer.chunkForConsumer:
				if !ok {
					return
				}

				halfChunkSize := len(chunk.offsetMessages) / 2
				for i, offMessage := range chunk.offsetMessages {
					consumer.setCurrentOffset(offMessage.offset)
					if canDispatch(offMessage) {
						consumer.MessagesHandler(ConsumerContext{Consumer: consumer, chunkInfo: &chunk}, offMessage.message)
					}

					// when half of the chunk is reached ask for a credit
					if halfChunkSize == i && consumer.options.CreditStrategy == AutomaticCreditStrategy {
						c.credit(consumer.id, 1)
					}

					if consumer.options.autocommit {
						messageCountBeforeStorage := consumer.increaseMessageCountBeforeStorage()
						if messageCountBeforeStorage >= consumer.options.autoCommitStrategy.messageCountBeforeStorage ||
							time.Since(consumer.getLastAutoCommitStored()) >= consumer.options.autoCommitStrategy.flushInterval {
							consumer.cacheStoreOffset()
						}
					}
				}

			case <-time.After(1_000 * time.Millisecond):
				if consumer.options.autocommit && time.Since(consumer.getLastAutoCommitStored()) >= consumer.options.autoCommitStrategy.flushInterval {
					consumer.cacheStoreOffset()
				}

				// This is a very edge case where the consumer is not active anymore
				// but the consumer is still in the list of consumers
				// It can happen during the reconnection with load-balancing
				// found this problem with a caos test where random killing the load-balancer and node where
				// the client should be connected
				if consumer.isZombie() {
					logs.LogWarn("Detected zombie consumer for stream %s, closing", streamName)
					consumer.close(Event{
						Command:    CommandUnsubscribe,
						StreamName: consumer.GetStreamName(),
						Name:       consumer.GetName(),
						Reason:     ZombieConsumer,
						Err:        nil,
					})
					return
				}
			}
		}
	}()
	return consumer, err.Err
}

func (c *Client) StreamStats(streamName string) (*StreamStats, error) {
	resp := c.coordinator.NewResponse(commandStreamStatus)
	correlationId := resp.correlationid

	length := 2 + 2 + 4 + 2 + len(streamName)

	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	writeProtocolHeader(b, length, commandStreamStatus,
		correlationId)
	writeString(b, streamName)

	err := c.handleWriteWithResponse(b.Bytes(), resp, false)
	if err.Err != nil {
		return nil, err.Err
	}

	offset := <-resp.data
	_ = c.coordinator.RemoveResponseById(resp.correlationid)
	m, ok := offset.(map[string]int64)
	if !ok {
		return nil, fmt.Errorf("invalid response, expected map[string]int64 but got %T", offset)
	}
	return newStreamStats(m, streamName), nil
}

func (c *Client) DeclareSuperStream(superStream string, options SuperStreamOptions) error {
	if !c.availableFeatures.is313OrMore {
		return fmt.Errorf("declaring super stream via client API not supported, server version is less than 3.13.0")
	}

	if superStream == "" || containsOnlySpaces(superStream) {
		return fmt.Errorf("super Stream Name can't be empty")
	}

	if options == nil {
		return fmt.Errorf("options can't be nil")
	}

	if len(options.getPartitions(superStream)) == 0 {
		return fmt.Errorf("partitions can't be empty")
	}

	if len(options.getBindingKeys()) == 0 {
		return fmt.Errorf("binding keys can't be empty")
	}

	for _, key := range options.getBindingKeys() {
		if key == "" || containsOnlySpaces(key) {
			return fmt.Errorf("binding key can't be empty")
		}
	}

	for _, partition := range options.getPartitions(superStream) {
		if partition == "" || containsOnlySpaces(partition) {
			return fmt.Errorf("partition can't be empty")
		}
	}

	length := 2 + 2 + 4 +
		2 + len(superStream) + 4 +
		sizeOfStringArray(options.getPartitions(superStream)) + 4 +
		sizeOfStringArray(options.getBindingKeys()) + 4 +
		sizeOfMapStringString(options.getArgs())

	resp := c.coordinator.NewResponse(commandCreateSuperStream, superStream)
	correlationId := resp.correlationid
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	writeProtocolHeader(b, length, commandCreateSuperStream, correlationId)
	writeString(b, superStream)
	writeStringArray(b, options.getPartitions(superStream))
	writeStringArray(b, options.getBindingKeys())
	writeMapStringString(b, options.getArgs())

	return c.handleWrite(b.Bytes(), resp).Err
}

func (c *Client) DeleteSuperStream(superStream string) error {
	if !c.availableFeatures.is313OrMore {
		return fmt.Errorf("deleting super stream not supported via client API, server version is less than 3.13.0")
	}

	length := 2 + 2 + 4 + 2 + len(superStream)
	resp := c.coordinator.NewResponse(commandDeleteSuperStream, superStream)
	correlationId := resp.correlationid
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	writeProtocolHeader(b, length, commandDeleteSuperStream,
		correlationId)
	writeString(b, superStream)
	return c.handleWrite(b.Bytes(), resp).Err
}

func (c *Client) QueryPartitions(superStream string) ([]string, error) {
	length := 2 + 2 + 4 + 2 + len(superStream)
	resp := c.coordinator.NewResponse(commandQueryPartition, superStream)
	correlationId := resp.correlationid
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	writeProtocolHeader(b, length, commandQueryPartition,
		correlationId)
	writeString(b, superStream)
	err := c.handleWriteWithResponse(b.Bytes(), resp, false)
	if err.Err != nil {
		return nil, err.Err
	}

	data := <-resp.data
	_ = c.coordinator.RemoveResponseById(resp.correlationid)
	return data.([]string), nil
}

func (c *Client) queryRoute(superStream string, routingKey string) ([]string, error) {
	length := 2 + 2 + 4 + 2 + len(superStream) + 2 + len(routingKey)
	resp := c.coordinator.NewResponse(commandQueryRoute, superStream)
	correlationId := resp.correlationid
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	writeProtocolHeader(b, length, commandQueryRoute,
		correlationId)
	writeString(b, routingKey)
	writeString(b, superStream)
	err := c.handleWriteWithResponse(b.Bytes(), resp, false)
	if err.Err != nil {
		return nil, err.Err
	}

	data := <-resp.data
	_ = c.coordinator.RemoveResponseById(resp.correlationid)
	return data.([]string), nil
}
