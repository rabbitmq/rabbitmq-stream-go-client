package raw

import (
	"context"
	"errors"
	"fmt"
	"github.com/go-logr/logr"
	"github.com/gsantomaggio/rabbitmq-stream-go-client/internal"
	"github.com/gsantomaggio/rabbitmq-stream-go-client/pkg/common"
	"math"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

type correlation struct {
	id         uint32
	chResponse chan internal.CommandRead
}

func newCorrelation(id uint32) *correlation {
	return &correlation{chResponse: make(chan internal.CommandRead),
		id: id}
}

func (c *correlation) Close() {
	// TODO: maybe we don't need to close the channel. When there are 0 references to
	// 		correlation struct, it will be garbage collected by Go's GC
	close(c.chResponse)
}

// Client is the base struct to interact with RabbitMQ streams at a low level. Client implements the common.Clienter
// interface. Functions of this interface accept a context.Context. It is highly advised to provide a context with a
// deadline/timeout to all function calls. When a context is cancelled, the function will cancel its work and return
// a relevant error.
type Client struct {
	mu sync.Mutex
	// this channel is used for correlation-less incoming frames from the server
	frameBodyListener    chan internal.CommandRead
	isOpen               bool
	connection           *internal.Connection
	correlationsMap      sync.Map
	nextCorrelation      uint32
	configuration        *ClientConfiguration
	connectionProperties map[string]string
}

func (tc *Client) IsOpen() bool {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	return tc.isOpen
}

// NewClient returns a common.Clienter implementation to interact with RabbitMQ stream using low level primitives.
// NewClient requires an established net.Conn and a ClientConfiguration. Using DialConfig() is the preferred method
// to establish a connection to RabbitMQ servers.
func NewClient(connection net.Conn, configuration *ClientConfiguration) common.Clienter {
	rawClient := &Client{
		frameBodyListener: make(chan internal.CommandRead),
		connection:        internal.NewConnection(connection),
		isOpen:            false,
		correlationsMap:   sync.Map{},
		configuration:     configuration,
	}
	return rawClient
}

// correlation map section

func (tc *Client) getCorrelationById(id uint32) *correlation {
	if v, ok := tc.correlationsMap.Load(id); ok {
		return v.(*correlation)
	}
	return nil
}

func (tc *Client) storeCorrelation(request internal.CommandWrite) {
	request.SetCorrelationId(tc.getNextCorrelation())
	tc.correlationsMap.Store(request.CorrelationId(), newCorrelation(request.CorrelationId()))
}

func (tc *Client) removeCorrelation(ctx context.Context, id uint32) {
	logger := logr.FromContextOrDiscard(ctx).WithName("correlation-map")
	corr := tc.getCorrelationById(id)
	if corr == nil {
		logger.Info("correlation not found, skipping removal", "correlation-id", id)
		return
	}
	corr.Close()
	tc.correlationsMap.Delete(id)
}

func (tc *Client) getNextCorrelation() uint32 {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.nextCorrelation += 1
	return tc.nextCorrelation
}

// end correlation map section

func (tc *Client) writeCommand(request internal.CommandWrite) error {
	hWritten, err := internal.NewHeaderRequest(request).Write(tc.connection.GetWriter())
	if err != nil {
		return err
	}
	bWritten, err := request.Write(tc.connection.GetWriter())
	if err != nil {
		return err
	}
	if (bWritten + hWritten) != (request.SizeNeeded() + 4) {
		panic("Write Command: Not all bytes written")
	}
	return tc.connection.GetWriter().Flush()
}

// This makes an RPC-style request. We send the frame, and we await for a response. The context is passed down from the
// public functions. Context should have a deadline/timeout to avoid deadlocks on a non-responding RabbitMQ server.
func (tc *Client) request(ctx context.Context, request internal.CommandWrite) (internal.CommandRead, error) {
	if ctx == nil {
		return nil, errNilContext
	}
	logger := logr.FromContextOrDiscard(ctx)

	tc.storeCorrelation(request)
	defer tc.removeCorrelation(ctx, request.CorrelationId())

	logger.V(traceLevel).Info("writing command to the wire", "request", request)
	err := tc.writeCommand(request)
	if err != nil {
		return nil, err
	}

	_, ok := ctx.Deadline()
	if !ok {
		logger.Info("request does not have a timeout, consider adding a deadline to context")
	}
	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("timed out waiting for server response")
	case r := <-tc.getCorrelationById(request.CorrelationId()).chResponse:
		return r, nil
	}
}

func (tc *Client) handleResponse(ctx context.Context, read internal.CommandRead) {
	var logger logr.Logger
	if ctx != nil {
		logger = logr.FromContextOrDiscard(ctx)
	} else {
		logger = logr.Discard()
	}

	correlation := tc.getCorrelationById(read.CorrelationId())
	if correlation == nil {
		logger.V(debugLevel).Info(
			"no correlation found for response",
			"response correlation",
			read.CorrelationId(),
		)
		return
	}
	// Perhaps we should check here ctx.Done() ?
	correlation.chResponse <- read
}

// TODO: Maybe Add a timeout
func (tc *Client) handleIncoming(ctx context.Context) error {
	if ctx == nil {
		return errNilContext
	}
	log := logr.FromContextOrDiscard(ctx).WithName("handleIncoming")
	buffer := tc.connection.GetReader()
	for {
		select {
		case <-ctx.Done():
			log.Info("context cancelled", "reason", ctx.Err())
			return ctx.Err()
		default:
			var header = new(internal.Header)
			// TODO: set an I/O deadline to avoid deadlock on I/O
			// 		renew the deadline at the beginning of each iteration
			err := header.Read(buffer)
			if err != nil {
				// TODO: some errors may be recoverable. We only need to return if reconnection
				// 	is needed
				log.Error(err, "error reading header for incoming frame")
				return err
			}
			switch internal.ExtractCommandCode(header.Command()) {
			case internal.CommandPeerProperties:
				peerPropResponse := internal.NewPeerPropertiesResponse()
				err = peerPropResponse.Read(buffer)
				if err != nil {
					log.Error(err, "error decoding peer properties")
					return err
				}
				tc.handleResponse(ctx, peerPropResponse)
			case internal.CommandSaslHandshake:
				saslMechanismsResponse := internal.NewSaslHandshakeResponse()
				err = saslMechanismsResponse.Read(buffer)
				if err != nil {
					log.Error(err, "error decoding SASL handshake")
					return err
				}
				tc.handleResponse(ctx, saslMechanismsResponse)
			case internal.CommandSaslAuthenticate:
				saslAuthResp := new(internal.SaslAuthenticateResponse)
				err = saslAuthResp.Read(buffer)
				if err != nil {
					log.Error(err, "error decoding SASL authenticate")
					return err
				}
				tc.handleResponse(ctx, saslAuthResp)
			case internal.CommandTune:
				tuneReq := new(internal.TuneRequest)
				err = tuneReq.Read(buffer)
				if err != nil {
					log.Error(err, "error decoding Tune")
					return err
				}
				tc.frameBodyListener <- tuneReq
			case internal.CommandOpen:
				openResp := new(internal.OpenResponse)
				err = openResp.Read(buffer)
				if err != nil {
					log.Error(err, "error decoding open")
					return err
				}
				tc.handleResponse(ctx, openResp)
			case internal.CommandClose:
				// FIXME: we may receive the request from the server
				// 		in such case, we have to start the shutdown process
				// 		we should stop decoding they key ID
				closeResp := new(internal.CloseResponse)
				err = closeResp.Read(buffer)
				if err != nil {
					log.Error(err, "error decoding close")
					return err
				}
				tc.handleResponse(ctx, closeResp)
			case internal.CommandCreate, internal.CommandDelete:
				createResp := new(internal.SimpleResponse)
				err = createResp.Read(buffer)
				if err != nil {
					log.Error(err, "error decoding simple response")
					return err
				}
				tc.handleResponse(ctx, createResp)
			default:
				log.Info("frame not implemented", "command ID", header.Command())
			}
		}
	}
}

func (tc *Client) peerProperties(ctx context.Context) error {
	if ctx == nil {
		return errNilContext
	}
	log := logr.FromContextOrDiscard(ctx).WithName("peer properties")

	serverPropertiesResponse, err := tc.request(ctx, internal.NewPeerPropertiesRequest())
	if err != nil {
		log.Error(err, "error in request to server")
		return err
	}
	response, ok := serverPropertiesResponse.(*internal.PeerPropertiesResponse)
	if !ok {
		panic("could not polymorph response")
	}
	log.V(debugLevel).Info(
		"peer properties response",
		"properties",
		response.ServerProperties,
	)
	return err
}

func (tc *Client) saslHandshake(ctx context.Context) error {
	if ctx == nil {
		return errNilContext
	}
	log := logr.FromContextOrDiscard(ctx).WithName("sasl handshake")
	saslMechanisms, err := tc.request(ctx, internal.NewSaslHandshakeRequest())
	saslMechanismResponse, ok := saslMechanisms.(*internal.SaslHandshakeResponse)
	if !ok {
		panic("could not polymorph response")
	}
	log.V(debugLevel).Info(
		"SASL mechanism response received",
		"mechanism",
		saslMechanismResponse.Mechanisms,
	)
	if err != nil {
		return err
	}

	// fixme: check response code
	tc.configuration.authMechanism = saslMechanismResponse.Mechanisms
	return nil
}

func (tc *Client) saslAuthenticate(ctx context.Context) error {
	// FIXME: make this pluggable to allow different authentication backends
	log := logr.FromContextOrDiscard(ctx).WithName("sasl authenticate")
	for _, mechanism := range tc.configuration.authMechanism {
		if strings.EqualFold(mechanism, "PLAIN") {
			log.V(debugLevel).Info("found PLAIN mechanism as supported")
			// FIXME: try different rabbitmq credentials
			saslPlain := internal.NewSaslPlainMechanism(
				tc.configuration.rabbitmqBrokers[0].Username,
				tc.configuration.rabbitmqBrokers[0].Password,
			)
			saslAuthReq := internal.NewSaslAuthenticateRequest(mechanism)
			err := saslAuthReq.SetChallengeResponse(saslPlain)
			if err != nil {
				log.Error(err, "error setting challenge response")
				return err
			}

			_, err = tc.request(ctx, saslAuthReq)
			if err != nil {
				log.Error(err, "error in SASL authenticate request")
				return err
			}
			//if saslAuthResp.ResponseCode() != internal.ResponseCodeOK {
			//	errCode, ok := responseCodeToError[saslAuthResp.ResponseCode()]
			//	if !ok {
			//		// we should never enter this
			//		return fmt.Errorf("unknown error code %d", saslAuthResp.ResponseCode())
			//	}
			//	return fmt.Errorf("error code %d: %w", saslAuthResp.ResponseCode(), errCode)
			//}
			return nil
		}
	}
	return errors.New("server does not support PLAIN SASL mechanism")
}

func (tc *Client) open(ctx context.Context, brokerIndex int) error {
	if ctx == nil {
		return errNilContext
	}
	log := logr.FromContextOrDiscard(ctx).WithName("open")
	rabbit := tc.configuration.rabbitmqBrokers[brokerIndex]
	openReq := internal.NewOpenRequest(rabbit.Vhost)
	openRespCommand, err := tc.request(ctx, openReq)
	if err != nil {
		log.Error(err, "error in open request")
		return err
	}
	// TODO check response code
	openResp, ok := openRespCommand.(*internal.OpenResponse)
	if !ok {
		panic("could not polymorph response")
	}
	log.V(debugLevel).Info(
		"open request success",
		"connection properties",
		openResp.ConnectionProperties(),
	)
	tc.connectionProperties = openResp.ConnectionProperties()

	return nil
}

// public API

// DialConfig establishes a connection to RabbitMQ servers in common.Configuration. It returns an error if the
// connection cannot be established. On a successful connection, in returns an implementation of common.Clienter,
// capable of interacting to RabbitMQ streams binary protocol at a low level.
//
// This is the recommended method to connect to RabbitMQ. After this function returns, a connection is established
// and authenticated with RabbitMQ. Do NOT call Client.Connect() after this function.
//
// ClientConfiguration must not be nil. ClientConfiguration should be initialised using NewClientConfiguration().
// A custom dial function can be set using ClientConfiguration.SetDial(). Check ClientConfiguration.SetDial()
// for more information. If dial function is not provided, DefaultDial is used with a timeout of 30 seconds.
// DefaultDial uses net.Dial
func DialConfig(ctx context.Context, config *ClientConfiguration) (common.Clienter, error) {
	// FIXME: test this code path at system level
	// FIXME: try to test this with net.Pipe fake
	//		dialer should return the fakeClientConn from net.Pipe
	if config == nil {
		return nil, errNilConfig
	}
	if ctx == nil {
		return nil, errNilContext
	}
	log := logr.FromContextOrDiscard(ctx).WithName("dial")
	var err error
	var conn net.Conn

	dialer := config.dial
	if dialer == nil {
		log.V(debugLevel).Info("no dial function provided, using default Dial")
		dialer = DefaultDial(defaultConnectionTimeout)
	}

	// TODO: TLS if scheme is rabbitmq-stream+tls
	for _, rabbitmqBroker := range config.rabbitmqBrokers {
		addr := net.JoinHostPort(rabbitmqBroker.Host, strconv.FormatInt(int64(rabbitmqBroker.Port), 10))
		// TODO: check if context is Done()
		conn, err = dialer("tcp", addr)
		if err != nil {
			log.Error(
				err,
				"failed to dial RabbitMQ, will try to dial another broker",
				"hostname",
				rabbitmqBroker.Host,
				"port",
				rabbitmqBroker.Port,
			)
			continue
		}
		break
	}

	if conn == nil {
		err := errors.New("failed to dial RabbitMQ")
		log.Error(err, "no more brokers to try")
		return nil, err
	}

	client := NewClient(conn, config)
	err = client.Connect(ctx)
	if err != nil {
		return nil, err
	}

	return client, nil
}

// DefaultDial establishes a connection when config.Dial is not provided
func DefaultDial(connectionTimeout time.Duration) func(network string, addr string) (net.Conn, error) {
	return func(network, addr string) (net.Conn, error) {
		conn, err := net.DialTimeout(network, addr, connectionTimeout)
		if err != nil {
			return nil, err
		}

		// Heartbeating hasn't started yet, don't stall forever on a dead server.
		// A deadline is set for TLS and Stream handshaking. After conn is established,
		// the deadline is cleared in Client.Connect().
		if err := conn.SetDeadline(time.Now().Add(connectionTimeout)); err != nil {
			return nil, err
		}

		return conn, nil
	}
}

// Connect performs a Stream-protocol handshake to connect to RabbitMQ. On a
// successful connect, it returns a nil error and starts listening to incoming
// frames from RabbitMQ. If more than 1 broker is defined in ClientConfiguration,
// they will be tried sequentially.
//
// It is recommended to establish a connection via DialConfig, instead of calling
// this method.
//
// Calling this method requires to manually establish a TCP connection to
// RabbitMQ, and create a Client using NewClient passing said established
// connection. It's recommended to use DialConfig instead.
func (tc *Client) Connect(ctx context.Context) error {
	if ctx == nil {
		return errNilContext
	}

	logger := logr.FromContextOrDiscard(ctx).WithName("connect")
	logger.Info("starting connection")

	var i = 0 // broker index for chosen broker to dial to

	go func(ctx context.Context) {
		log := logr.FromContextOrDiscard(ctx).WithName("frame-listener")
		log.V(debugLevel).Info("starting frame listener")
		err := tc.handleIncoming(ctx)
		if err != nil {
			log.Error(err, "error handling incoming frames")
		}
	}(ctx)

	// TODO: stop handshake if context is Done()
	err := tc.peerProperties(ctx)
	if err != nil {
		// FIXME: wrap error in Connect-specific error
		logger.Error(err, "error exchanging peer properties")
		return err
	}
	err = tc.saslHandshake(ctx)
	if err != nil {
		logger.Error(err, "error in SASL handshake")
		return err
	}

	err = tc.saslAuthenticate(ctx)
	if err != nil {
		logger.Error(err, "error in SASL authenticate")
		return err
	}

	logger.V(debugLevel).Info("awaiting Tune frame from server")
	tuneCtx, tuneCancel := context.WithTimeout(ctx, time.Second*30)
	select {
	case <-tuneCtx.Done():
		logger.Error(tuneCtx.Err(), "error awaiting for tune from server")
		tuneCancel()
		return tuneCtx.Err()
	case tuneReqCommand := <-tc.frameBodyListener:
		tuneReq, ok := tuneReqCommand.(*internal.TuneRequest)
		if !ok {
			panic("could not polymorph CommandRead into TuneRequest")
		}

		desiredFrameSize := math.Min(float64(tuneReq.FrameMaxSize()), float64(tc.configuration.clientMaxFrameSize))
		desiredHeartbeat := math.Min(float64(tuneReq.HeartbeatPeriod()), float64(tc.configuration.clientHeartbeat))

		logger.V(debugLevel).Info(
			"desired tune options",
			"frame-size",
			desiredFrameSize,
			"heartbeat",
			desiredHeartbeat,
		)
		tuneResp := internal.NewTuneResponse(uint32(desiredFrameSize), uint32(desiredHeartbeat))
		err = tc.writeCommand(tuneResp)
		if err != nil {
			logger.Error(err, "error in Tune")
			tuneCancel()
			return err
		}
	}
	tuneCancel()

	err = tc.open(ctx, i)
	if err != nil {
		logger.Error(err, "error in open")
		return err
	}

	// clear any deadline set by Dial functions.
	// Dial functions may set an i/o timeout for TLS and AMQP handshake.
	// Such timeout should not apply to stream i/o operations
	logger.V(debugLevel).Info("clearing connection I/O deadline")
	err = tc.connection.SetDeadline(time.Time{})
	if err != nil {
		logger.Error(err, "error setting connection I/O deadline")
		return err
	}

	tc.mu.Lock()
	tc.isOpen = true
	defer tc.mu.Unlock()
	logger.Info("connection is open")

	return nil
}

// DeclareStream sends a request to create a new Stream. If the error is nil, the
// Stream was created successfully, and it is ready to use.
func (tc *Client) DeclareStream(ctx context.Context, stream string, configuration common.StreamConfiguration) error {
	if ctx == nil {
		return errNilContext
	}

	log := logr.FromContextOrDiscard(ctx).WithName("DeclareStream")
	log.V(debugLevel).Info("starting declare stream. ", "stream", stream)

	createResponse, err := tc.request(ctx, internal.NewCreateRequest(stream, configuration))
	if err != nil {
		log.Error(err, "error creating declare stream request ")
		return err
	}
	return streamErrorOrNil(createResponse.ResponseCode())
}

// DeleteStream sends a request to delete a Stream. If the error is nil, the
// Stream was deleted successfully.
func (tc *Client) DeleteStream(ctx context.Context, stream string) error {
	if ctx == nil {
		return errNilContext
	}

	log := logr.FromContextOrDiscard(ctx).WithName("DeleteStream")
	log.V(debugLevel).Info("starting delete stream. ", "stream", stream)

	deleteResponse, err := tc.request(ctx, internal.NewDeleteRequest(stream))
	if err != nil {
		log.Error(err, "error creating delete stream request ")
		return err
	}
	return streamErrorOrNil(deleteResponse.ResponseCode())
}

// Close gracefully shutdowns the connection to RabbitMQ. The Client will send a
// close request to RabbitMQ, and it will await a response. It is recommended to
// set a deadline in the context, to avoid waiting forever on a non-responding
// RabbitMQ server.
func (tc *Client) Close(ctx context.Context) error {
	if ctx == nil {
		return errNilContext
	}

	log := logr.FromContextOrDiscard(ctx).WithName("close")
	log.V(debugLevel).Info("starting connection close")

	response, err := tc.request(ctx, internal.NewCloseRequest(internal.ResponseCodeOK, "kthxbye"))
	if err != nil {
		log.Error(err, "error sending close request")
		return err
	}

	// TODO: check response code
	log.V(debugLevel).Info("server response", "response code", response.ResponseCode())

	err = tc.connection.Close()
	if err != nil {
		log.Error(err, "error closing tcp connection")
		return err
	}

	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.isOpen = false

	return nil
}
