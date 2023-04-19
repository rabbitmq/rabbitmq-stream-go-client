package raw

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/go-logr/logr"
	"github.com/gsantomaggio/rabbitmq-stream-go-client/internal"
	"github.com/gsantomaggio/rabbitmq-stream-go-client/pkg/common"
	"github.com/gsantomaggio/rabbitmq-stream-go-client/pkg/constants"
	"io"
	"math"
	"net"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
)

type correlation struct {
	id         uint32
	chResponse chan internal.SyncCommandRead
}

func newCorrelation(id uint32) *correlation {
	return &correlation{chResponse: make(chan internal.SyncCommandRead),
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
// Client is not thread-safe. It is the responsibility of the caller to ensure that only one goroutine is using the
// Client at a time.
type Client struct {
	mu sync.Mutex
	// this channel is used for correlation-less incoming frames from the server
	frameBodyListener    chan internal.SyncCommandRead
	isOpen               bool
	connection           *internal.Connection
	correlationsMap      sync.Map
	nextCorrelation      uint32
	configuration        *ClientConfiguration
	connectionProperties map[string]string
	confirmsCh           chan *PublishConfirm
	chunkCh              chan *Chunk
	notifyCh             chan *CreditError
	metadataCh           chan *MetadataResponse
}

// IsOpen returns true if the connection is open, false otherwise
// IsOpen is thread-safe
func (tc *Client) IsOpen() bool {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	return tc.isOpen
}

// NewClient returns a common.Clienter implementation to interact with RabbitMQ stream using low level primitives.
// NewClient requires an established net.Conn and a ClientConfiguration. Using DialConfig() is the preferred method
// to establish a connection to RabbitMQ servers.
func NewClient(connection net.Conn, configuration *ClientConfiguration) Clienter {
	rawClient := &Client{
		frameBodyListener: make(chan internal.SyncCommandRead),
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

func (tc *Client) storeCorrelation(request internal.SyncCommandWrite) {
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

// This makes an RPC-style syncRequest. We send the frame, and we await for a response. The context is passed down from the
// public functions. Context should have a deadline/timeout to avoid deadlocks on a non-responding RabbitMQ server.
func (tc *Client) syncRequest(ctx context.Context, request internal.SyncCommandWrite) (internal.SyncCommandRead, error) {
	if ctx == nil {
		return nil, errNilContext
	}

	select {
	default:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	logger := logr.FromContextOrDiscard(ctx).WithName("syncRequest")

	tc.storeCorrelation(request)
	defer tc.removeCorrelation(ctx, request.CorrelationId())

	logger.V(traceLevel).Info("writing sync command to the wire", "syncRequest", request)
	err := internal.WriteCommand(request, tc.connection.GetWriter())
	if err != nil {
		logger.Error(err, "error writing command to the wire")
		return nil, err
	}

	if _, ok := ctx.Deadline(); !ok {
		logger.Info("syncRequest does not have a timeout, consider adding a deadline to context")
	}
	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("timed out waiting for server response")
	case r := <-tc.getCorrelationById(request.CorrelationId()).chResponse:
		return r, nil
	}
}

// This makes an async-style syncRequest. We send the frame, and we don't wait for a response. The context is passed down from the
// public functions. Context should have a deadline/timeout to avoid deadlocks on a non-responding RabbitMQ server.
// `syncRequest` is the frame to send to the server that does not expect a response.
func (tc *Client) request(ctx context.Context, request internal.CommandWrite) error {
	if ctx == nil {
		return errNilContext
	}
	// TODO: check if context is canceled before proceeding to WriteCommand

	select {
	default:
	case <-ctx.Done():
		return ctx.Err()
	}

	logger := logr.FromContextOrDiscard(ctx)
	logger.V(traceLevel).Info("writing command to the wire", "syncRequest", request)
	return internal.WriteCommand(request, tc.connection.GetWriter())
}

// handleResponse is responsible for handling incoming frames from the server.
// in this case the call is synchronous, so we need to correlate the response with the request
// and send it to the right channel that is waiting for it.
func (tc *Client) handleResponse(ctx context.Context, read internal.SyncCommandRead) {
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
// handleIncoming is responsible for handling incoming frames from the server.
// each frame is handled by a specific handler function.
// the frame must read the internal.Header to decode the frame body.
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
			// 		clear the deadline after reading the header
			err := header.Read(buffer)
			if errors.Is(err, io.ErrClosedPipe) {
				return nil
			}
			if err != nil {
				// TODO: some errors may be recoverable. We only need to return if reconnection
				// 	is needed
				log.Error(err, "error reading header for incoming frame")
				return err
			}
			switch header.Command() {
			case internal.CommandPeerPropertiesResponse:
				peerPropResponse := internal.NewPeerPropertiesResponse()
				err = peerPropResponse.Read(buffer)
				if err != nil {
					log.Error(err, "error decoding peer properties")
					return err
				}
				tc.handleResponse(ctx, peerPropResponse)
			case internal.CommandSaslHandshakeResponse:
				saslMechanismsResponse := internal.NewSaslHandshakeResponse()
				err = saslMechanismsResponse.Read(buffer)
				if err != nil {
					log.Error(err, "error decoding SASL handshake")
					return err
				}
				tc.handleResponse(ctx, saslMechanismsResponse)
			case internal.CommandSaslAuthenticateResponse:
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
			case internal.CommandOpenResponse:
				openResp := new(internal.OpenResponse)
				err = openResp.Read(buffer)
				if err != nil {
					log.Error(err, "error decoding open")
					return err
				}
				tc.handleResponse(ctx, openResp)
			case internal.CommandClose:
				// server closed the connection
				closeReq := new(internal.CloseRequest)
				err = closeReq.Read(buffer)
				if err != nil {
					log.Error(err, "error decoding close syncRequest")
					return err
				}

				log.Info("server requested connection close", "close-reason", closeReq.ClosingReason())
				err = tc.handleClose(ctx, closeReq)
				if err != nil {
					log.Error(err, "error sending close response")
					// we do not return here because we must execute the shutdown process and close the socket
				}

				err = tc.shutdown()
				if err != nil && !errors.Is(err, io.ErrClosedPipe) {
					log.Error(err, "error in shutdown")
					return err
				}
				return nil
			case internal.CommandCreateResponse,
				internal.CommandDeleteResponse,
				internal.CommandDeclarePublisherResponse,
				internal.CommandDeletePublisherResponse,
				internal.CommandCloseResponse,
				internal.CommandSubscribeResponse:
				createResp := new(internal.SimpleResponse)
				err = createResp.Read(buffer)
				if err != nil {
					log.Error(err, "error decoding simple response")
					return err
				}
				tc.handleResponse(ctx, createResp)
			case internal.CommandPublishConfirm:
				publishConfirm := new(internal.PublishConfirmResponse)
				err = publishConfirm.Read(buffer)
				if err != nil {
					log.Error(err, "error decoding publish confirm")
					return err
				}

				if tc.confirmsCh == nil {
					log.Info("confirmation channel is not registered. Use Client.NotifyPublish() to receive publish confirmations")
					continue
				}

				select {
				case <-ctx.Done():
					return ctx.Err()
				case tc.confirmsCh <- publishConfirm:
					log.V(debugLevel).Info("sent a publish confirm", "publisherId", publishConfirm.PublisherID())
				}
			case internal.CommandDeliver:
				chunkResponse := new(internal.ChunkResponse)
				err = chunkResponse.Read(buffer)
				log.V(debugLevel).Info("received a chunk", "chunk", chunkResponse.Timestamp)
				if err != nil {
					log.Error(err, "error decoding chunk Response")
					return err
				}
				if tc.chunkCh == nil {
					log.Info("chunk channel is not registered. Use Client.NotifyChunk() to receive chunks")
					continue
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case tc.chunkCh <- chunkResponse:
					log.V(debugLevel).Info("sent a subscription chunk", "subscriptionId", chunkResponse.SubscriptionId)
				}
			case internal.CommandExchangeCommandVersionsResponse:
				exchangeResponse := new(internal.ExchangeCommandVersionsResponse)
				err = exchangeResponse.Read(buffer)
				log.V(debugLevel).Info("received exchange command versions response")
				if err != nil {
					log.Error(err, "error ")
				}
				tc.handleResponse(ctx, exchangeResponse)
			case internal.CommandQueryOffsetResponse:
				queryOffsetResponse := new(internal.QueryOffsetResponse)
				err = queryOffsetResponse.Read(buffer)
				log.V(debugLevel).Info("received query offset response")
				if err != nil {
					log.Error(err, "error during receiving query offset response")
				}
				tc.handleResponse(ctx, queryOffsetResponse)
			case internal.CommandCreditResponse:
				creditResp := new(CreditError)
				err = creditResp.Read(buffer)
				log.Error(
					errUnknownSubscription,
					"received credit response for unknown subscription",
					"responseCode",
					creditResp.ResponseCode(),
					"subscriptionId",
					creditResp.SubscriptionId(),
				)
				if err != nil {
					log.Error(err, "error in credit response")
					return err
				}

				tc.mu.Lock()
				if tc.notifyCh != nil {
					select {
					case <-ctx.Done():
						tc.mu.Unlock()
						return ctx.Err()
					case tc.notifyCh <- creditResp:
					}
				}
				tc.mu.Unlock()
			case internal.CommandMetadataResponse:
				metadataResp := new(internal.MetadataResponse)
				err := metadataResp.Read(buffer)
				log.V(debugLevel).Info("received metadata response")
				if err != nil {
					log.Error(err, "error ")
				}
				tc.handleResponse(ctx, metadataResp)
			case internal.CommandStreamStatsResponse:
				streamStatsResp := new(internal.StreamStatsResponse)
				err := streamStatsResp.Read(buffer)
				log.V(debugLevel).Info("received stream stats response")
				if err != nil {
					log.Error(err, "error ")
				}
				tc.handleResponse(ctx, streamStatsResp)
			default:
				log.Info("frame not implemented", "command ID", fmt.Sprintf("%X", header.Command()))
				_, err := buffer.Discard(header.Length() - 4)
				if err != nil {
					log.V(debugLevel).Error(err, "error discarding bytes from unknown frame", "discard", int(header.Length()-4))
				}
			}
		}
	}
}

func (tc *Client) peerProperties(ctx context.Context) error {
	if ctx == nil {
		return errNilContext
	}
	log := logr.FromContextOrDiscard(ctx).WithName("peer properties")
	log.V(traceLevel).Info("starting peer properties")

	serverPropertiesResponse, err := tc.syncRequest(ctx, internal.NewPeerPropertiesRequest())
	if err != nil {
		log.Error(err, "error in syncRequest to server")
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
	return streamErrorOrNil(response.ResponseCode())
}

func (tc *Client) saslHandshake(ctx context.Context) error {
	if ctx == nil {
		return errNilContext
	}
	log := logr.FromContextOrDiscard(ctx).WithName("sasl handshake")
	log.V(traceLevel).Info("starting SASL handshake")

	saslMechanisms, err := tc.syncRequest(ctx, internal.NewSaslHandshakeRequest())
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

	tc.configuration.authMechanism = saslMechanismResponse.Mechanisms
	return streamErrorOrNil(saslMechanismResponse.ResponseCode())
}

func (tc *Client) saslAuthenticate(ctx context.Context) error {
	// TODO: perhaps we could merge saslHandshake and saslAuthenticate in a single function?
	// FIXME: make this pluggable to allow different authentication backends
	log := logr.FromContextOrDiscard(ctx).WithName("sasl authenticate")
	log.V(traceLevel).Info("starting SASL authenticate")

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

			response, err := tc.syncRequest(ctx, saslAuthReq)
			if err != nil {
				// no need to log here. the caller logs the error
				return err
			}
			return streamErrorOrNil(response.ResponseCode())
		}
	}
	return errors.New("server does not support PLAIN SASL mechanism")
}

func (tc *Client) open(ctx context.Context, brokerIndex int) error {
	if ctx == nil {
		return errNilContext
	}
	log := logr.FromContextOrDiscard(ctx).WithName("open")
	log.V(traceLevel).Info("starting open")

	rabbit := tc.configuration.rabbitmqBrokers[brokerIndex]
	openReq := internal.NewOpenRequest(rabbit.Vhost)
	openRespCommand, err := tc.syncRequest(ctx, openReq)
	if err != nil {
		log.Error(err, "error in open syncRequest")
		return err
	}

	openResp, ok := openRespCommand.(*internal.OpenResponse)
	if !ok {
		panic("could not polymorph response")
	}
	log.V(debugLevel).Info(
		"open syncRequest success",
		"connection properties",
		openResp.ConnectionProperties(),
	)
	tc.connectionProperties = openResp.ConnectionProperties()

	return streamErrorOrNil(openResp.ResponseCode())
}

func (tc *Client) shutdown() error {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.isOpen = false

	if tc.confirmsCh != nil {
		close(tc.confirmsCh)
	}

	if tc.chunkCh != nil {
		close(tc.chunkCh)
	}

	return tc.connection.Close()
}

func (tc *Client) handleClose(ctx context.Context, req *internal.CloseRequest) error {
	if ctx == nil {
		return errNilContext
	}

	select {
	default:
	case <-ctx.Done():
		return ctx.Err()
	}

	var n int
	hdr := internal.NewHeader(10, internal.CommandCloseResponse, 1)
	bytesWritten, err := hdr.Write(tc.connection.GetWriter())
	n += bytesWritten
	if err != nil {
		return err
	}

	bdy := internal.NewSimpleResponseWith(req.CorrelationId(), constants.ResponseCodeOK)
	b, err := bdy.MarshalBinary()
	if err != nil {
		return err
	}
	bytesWritten, err = tc.connection.GetWriter().Write(b)
	n += bytesWritten
	if err != nil {
		return err
	}
	return tc.connection.GetWriter().Flush()
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
//
// The context passed to DialConfig will be used with in the background routine that listens for incoming frames
// from the server. Cancelling this context will stop the background routine, causing the client to not read
// any further communication from the server. Something like this would cause the client to not function correctly:
//
//	func connectToRabbit(ctx context.Context) (common.Clienter, error) {
//		dialCtx, cancel := context.WithTimeout(ctx, time.Second)
//		defer cancel()
//		return raw.DialConfig(dialCtx, cfg)
//	}
//
// This will be addressed in [issue#27]
//
// The client returned by the above code will not function correctly, because the background routine won't
// read any information from the server (since the context is cancelled after return). If you want to implement
// a custom timeout or dial-retry mechanism, provide your own dial function using [raw.ClientConfiguration] SetDial().
// A simple dial function could be as follows:
//
//	cfg.SetDial(func(network, addr string) (net.Conn, error) {
//		return net.DialTimeout(network, addr, time.Second)
//	})
//
// [issue#27]: https://github.com/Gsantomaggio/rabbitmq-stream-go-client/issues/27
func DialConfig(ctx context.Context, config *ClientConfiguration) (Clienter, error) {
	// FIXME: try to test this with net.Pipe fake
	//		dialer should return the fakeClientConn from net.Pipe
	// FIXME: comment about context cancellation
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

	log := logr.FromContextOrDiscard(ctx).WithName("connect")
	log.Info("starting connection")

	var i = 0 // broker index for chosen broker to dial to

	// FIXME: handleIncoming should use its own context
	//		we are making an incorrect use of context
	// https://github.com/Gsantomaggio/rabbitmq-stream-go-client/issues/27
	go func(ctx context.Context) {
		log := logr.FromContextOrDiscard(ctx).WithName("frame-listener")
		log.V(traceLevel).Info("starting frame listener")

		select {
		default:
		case <-ctx.Done():
			log.Info("context cancelled", "err", ctx.Err())
			return
		}

		err := tc.handleIncoming(ctx)
		if err != nil {
			log.Error(err, "error handling incoming frames")
		}
	}(ctx)

	err := tc.peerProperties(ctx)
	if err != nil {
		// FIXME: wrap error in Connect-specific error
		log.Error(err, "error exchanging peer properties")
		return err
	}
	err = tc.saslHandshake(ctx)
	if err != nil {
		log.Error(err, "error in SASL handshake")
		return err
	}

	err = tc.saslAuthenticate(ctx)
	if err != nil {
		log.Error(err, "error in SASL authenticate")
		return err
	}

	log.V(debugLevel).Info("awaiting Tune frame from server")
	tuneCtx, tuneCancel := context.WithTimeout(ctx, time.Second*30)
	select {
	case <-tuneCtx.Done():
		log.Error(tuneCtx.Err(), "error awaiting for tune from server")
		tuneCancel()
		return tuneCtx.Err()
	case tuneReqCommand := <-tc.frameBodyListener:
		tuneReq, ok := tuneReqCommand.(*internal.TuneRequest)
		if !ok {
			panic("could not polymorph SyncCommandRead into TuneRequest")
		}

		desiredFrameSize := math.Min(float64(tuneReq.FrameMaxSize()), float64(tc.configuration.clientMaxFrameSize))
		desiredHeartbeat := math.Min(float64(tuneReq.HeartbeatPeriod()), float64(tc.configuration.clientHeartbeat))

		log.V(debugLevel).Info(
			"desired tune options",
			"frame-size",
			desiredFrameSize,
			"heartbeat",
			desiredHeartbeat,
		)
		tuneResp := internal.NewTuneResponse(uint32(desiredFrameSize), uint32(desiredHeartbeat))
		err = internal.WriteCommand(tuneResp, tc.connection.GetWriter())
		if err != nil {
			log.Error(err, "error in Tune")
			tuneCancel()
			return err
		}
	}
	tuneCancel()

	err = tc.open(ctx, i)
	if err != nil {
		log.Error(err, "error in open")
		return err
	}

	// clear any deadline set by Dial functions.
	// Dial functions may set an i/o timeout for TLS and AMQP handshake.
	// Such timeout should not apply to stream i/o operations
	log.V(debugLevel).Info("clearing connection I/O deadline")
	err = tc.connection.SetDeadline(time.Time{})
	if err != nil {
		log.Error(err, "error setting connection I/O deadline")
		return err
	}

	tc.mu.Lock()
	tc.isOpen = true
	defer tc.mu.Unlock()
	log.Info("connection is open")

	return nil
}

// DeclareStream sends a syncRequest to create a new Stream. If the error is nil, the
// Stream was created successfully, and it is ready to use.
// By default, the stream is created without any retention policy, so stream can grow indefinitely.
// It is recommended to set a retention policy to avoid filling up the disk.
// See also https://www.rabbitmq.com/streams.html#retention
func (tc *Client) DeclareStream(ctx context.Context, stream string, configuration constants.StreamConfiguration) error {
	if ctx == nil {
		return errNilContext
	}

	log := logr.FromContextOrDiscard(ctx).WithName("DeclareStream")
	log.V(debugLevel).Info("starting declare stream. ", "stream", stream)

	createResponse, err := tc.syncRequest(ctx, internal.NewCreateRequest(stream, configuration))
	if err != nil {
		log.Error(err, "error declaring stream", "stream", stream, "stream-args", configuration)
		return err
	}
	return streamErrorOrNil(createResponse.ResponseCode())
}

// DeleteStream sends a syncRequest to delete a Stream. If the error is nil, the
// Stream was deleted successfully.
func (tc *Client) DeleteStream(ctx context.Context, stream string) error {
	if ctx == nil {
		return errNilContext
	}

	log := logr.FromContextOrDiscard(ctx).WithName("DeleteStream")
	log.V(debugLevel).Info("starting delete stream. ", "stream", stream)

	deleteResponse, err := tc.syncRequest(ctx, internal.NewDeleteRequest(stream))
	if err != nil {
		log.Error(err, "error deleting stream", "stream", stream)
		return err
	}
	return streamErrorOrNil(deleteResponse.ResponseCode())
}

// DeclarePublisher sends a syncRequest to create a new Publisher. If the error is
// nil, the Publisher was created successfully.
// publisherId is the ID of the publisher to create. The publisherId is not tracked in this level of the client.
// The publisherId is used to identify the publisher in the server. Per connection
// publisherReference identify the publisher. it can be empty. With the publisherReference is possible to query the last published offset.
// stream is the name of the stream to publish to.
func (tc *Client) DeclarePublisher(ctx context.Context, publisherId uint8, publisherReference string, stream string) error {
	if ctx == nil {
		return errNilContext
	}

	log := logr.FromContextOrDiscard(ctx).WithName("DeclarePublisher")
	log.V(debugLevel).Info("starting declare publisher. ", "publisherId", publisherId, "publisherReference", publisherReference, "stream", stream)

	deleteResponse, err := tc.syncRequest(ctx, internal.NewDeclarePublisherRequest(publisherId, publisherReference, stream))
	if err != nil {
		log.Error(err, "error declaring publisher", "publisherId", publisherId, "publisherReference", publisherReference, "stream", stream)
		return err
	}
	return streamErrorOrNil(deleteResponse.ResponseCode())
}

// Send publishes a batch of messages for a given publisher. The messages to be
// published must have a publishing ID and a function to write the body to an
// io.Writer. The first parameter is a context.Context. The context will be
// checked before writing messages to the wire. This function publishes in a
// "fire and forget" fashion; this means it will not wait for a response from the
// server after sending the messages over the network.
//
// The publisher ID is the same ID used in a DeclarePublisher function call. The
// application must keep track of this ID for sending messages.
//
// The slice of common.PublishingMessager is a batch of messages to be sent. Note
// that RabbitMQ Stream protocol does not specify a format for the messages. This
// flexibility allows to send, in a single "Publish" frame, a batch of
// application messages; for example, a batch of AMQP 1.0 messages.
func (tc *Client) Send(ctx context.Context, publisherId uint8, publishingMessages []common.PublishingMessager) error {
	if ctx == nil {
		return errNilContext
	}

	logger := logr.FromContextOrDiscard(ctx).WithName("Send")
	logger.V(debugLevel).Info("starting send", "publisherId", publisherId, "message-count", len(publishingMessages))

	buff := new(bytes.Buffer)
	for _, msg := range publishingMessages {
		_, err := msg.WriteTo(buff)
		if err != nil {
			return err
		}
	}
	return tc.request(ctx, internal.NewPublishRequest(publisherId, uint32(len(publishingMessages)), buff.Bytes()))
}

// SendSubEntryBatch aggregates, compress and publishes a batch of messages.
// Similar to Send, but it aggregates the messages for a single publishingId.
// For a single publishingId there could be multiple messages (max ushort).
// The compression is done via common.CompresserCodec interface see common.CompresserCodec for more details.
// The use case for SendSubEntryBatch is to compress a batch of messages for example sending logs.

// SendSubEntryBatch is fire and forget, like the send function the client will receive the confirmation from the server.
// The server sends only the confirmation for the `publishingId`.

// The publishingId should be the last id of []common.PublishingMessager but we leave the api open to allow
// the caller to choose the publishingId.

func (tc *Client) SendSubEntryBatch(ctx context.Context, publisherId uint8,
	publishingId uint64, compress common.CompresserCodec, publishingMessages []common.PublishingMessager) error {
	if ctx == nil {
		return errNilContext
	}
	logger := logr.FromContextOrDiscard(ctx).WithName("SendSubEntryBatch")
	logger.V(debugLevel).Info("starting send-sub-entry", "publisherId", publisherId, "message-count", len(publishingMessages))

	buff := new(bytes.Buffer)
	for _, msg := range publishingMessages {
		_, err := msg.WriteTo(buff)
		if err != nil {
			return err
		}
	}

	uncompressedDataSize := len(buff.Bytes())

	compressed, err := compress.Compress(buff.Bytes())
	if err != nil {
		return err
	}
	compressedDataSize := len(compressed)

	return tc.request(ctx, internal.NewSubBatchPublishRequest(publisherId,
		1,
		publishingId,
		compress.GetType(),
		uint16(len(publishingMessages)),
		uncompressedDataSize,
		compressedDataSize,
		compressed))
}

// DeletePublisher sends a syncRequest to delete a Publisher. If the error is nil,
// the Publisher was deleted successfully.
// publisherId is the ID of the publisher to delete.
// publisherId is not tracked in this level of the client.
func (tc *Client) DeletePublisher(ctx context.Context, publisherId uint8) error {
	if ctx == nil {
		return errNilContext
	}

	log := logr.FromContextOrDiscard(ctx).WithName("DeletePublisher")
	log.V(debugLevel).Info("starting delete publisher. ", "publisherId", publisherId)

	deleteResponse, err := tc.syncRequest(ctx, internal.NewDeletePublisherRequest(publisherId))
	if err != nil {
		log.Error(err, "error creating delete publisher syncRequest ", "publisherId", publisherId)
		return err
	}
	return streamErrorOrNil(deleteResponse.ResponseCode())
}

// Subscribe sends a syncRequest to create a new Consumer. If the error is nil,
// the Consumer was created successfully. subscriptionId is the ID of the
// subscriber to create. The subscriptionId is not tracked in this level of the
// client. The subscriptionId is used to identify the consumer in the server.
// stream is the name of the stream to subscribe to. offsetType is the type of
// offset to start consuming from. See constants.OffsetType for more information.
//
//	OffsetTypeFirst     uint16 = 0x01 // Start from the first message in the stream
//	OffsetTypeLast      uint16 = 0x02 // Start from the last chunk in the stream
//	OffsetTypeNext      uint16 = 0x03 // Start from the next chunk in the stream
//	OffsetTypeOffset    uint16 = 0x04 // Start from a specific offset in the stream
//	OffsetTypeTimeStamp uint16 = 0x05 // Start from a specific timestamp in the stream
//
// offset is the offset to start consuming from.
// The offset is ignored if the offsetType is not OffsetTypeOffset or OffsetTypeTimeStamp.
// See also https://www.rabbitmq.com/streams.html#consuming for more information.
// properties is the configuration of the consumer. It is optional.
// With properties is possible to configure values like:
// - name of the consumer: "name" : "my-consumer"
// - Super Stream : "super-stream" : "my-super-stream"
// - Single Active Consumer: "single-active-consumer" : true
func (tc *Client) Subscribe(
	ctx context.Context,
	stream string,
	offsetType uint16,
	subscriptionId uint8,
	credit uint16,
	properties constants.SubscribeProperties,
	offset uint64,
) error {
	if ctx == nil {
		return errNilContext
	}

	log := logr.FromContextOrDiscard(ctx).WithName("subscribe")
	log.V(debugLevel).Info("starting declare consumer. ", "subscriptionId", subscriptionId, "stream", stream,
		"offsetType", offsetType, "offset", offset, "credit", credit, "properties", properties)

	subscribeResponse, err := tc.syncRequest(ctx, internal.NewSubscribeRequestRequest(subscriptionId, stream, offsetType, offset, credit, properties))
	if err != nil {
		log.Error(err, "error  declaring consumer. ", "subscriptionId", subscriptionId, "stream", stream,
			"offsetType", offsetType, "offset", offset, "credit", credit, "properties", properties)
		return err
	}
	return streamErrorOrNil(subscribeResponse.ResponseCode())
}

// Close gracefully shutdowns the connection to RabbitMQ. The Client will send a
// close syncRequest to RabbitMQ, and it will await a response. It is recommended to
// set a deadline in the context, to avoid waiting forever on a non-responding
// RabbitMQ server.
func (tc *Client) Close(ctx context.Context) error {
	if ctx == nil {
		return errNilContext
	}

	log := logr.FromContextOrDiscard(ctx).WithName("close")
	log.V(debugLevel).Info("starting connection close")

	response, err := tc.syncRequest(ctx, internal.NewCloseRequest(constants.ResponseCodeOK, "kthxbye"))
	if err != nil {
		log.Error(err, "error sending close syncRequest")
		return err
	}

	err = streamErrorOrNil(response.ResponseCode())
	log.V(debugLevel).Info("server response", "response code", response.ResponseCode())
	if err != nil {
		// TODO: should we continue with the shutdown process instead of returning?
		return err
	}

	err = tc.shutdown()
	if err != nil && !errors.Is(err, io.ErrClosedPipe) {
		log.Error(err, "error closing connection")
		return err
	}
	return nil
}

// ExchangeCommandVersions TODO: godocs
func (tc *Client) ExchangeCommandVersions(ctx context.Context) error {
	if ctx == nil {
		return errNilContext
	}

	logger := logr.FromContextOrDiscard(ctx).WithName("ExchangeCommandVersions")
	logger.V(debugLevel).Info("starting exchange command versions")
	response, err := tc.syncRequest(ctx, internal.NewExchangeCommandVersionsRequest())
	if err != nil {
		logger.Error(err, "error sending sync request for exchange command versions")
		return err
	}

	return streamErrorOrNil(response.ResponseCode())
}

// Credit TODO: go docs
func (tc *Client) Credit(ctx context.Context, subscriptionID uint8, credits uint16) error {
	if ctx == nil {
		return errNilContext
	}
	logger := logr.FromContextOrDiscard(ctx).WithName("Credit")
	logger.V(debugLevel).Info("starting credit")

	return tc.request(ctx, internal.NewCreditRequest(subscriptionID, credits))
}

// MetadataQuery sends a syncRequest to get metadata for a given stream. If the error is nil,
// the MetadataResponse is returned which has the following structure:-
//
//	MetadataResponse{
//		correlationID uint32
//		broker Broker{
//			reference uint16
//			host string
//			port uint32
//		}
//		streamMetadata StreamMetadata{
//			streamName string
//			responseCode uint16
//			leaderReference uint16
//			replicasReferences []uint16
//		}
//	}
//
// stream is the name of the stream.
func (tc *Client) MetadataQuery(ctx context.Context, stream string) (*MetadataResponse, error) {
	if ctx == nil {
		return nil, errNilContext
	}

	logger := logr.FromContextOrDiscard(ctx).WithName("MetadataQuery")
	logger.V(debugLevel).Info("starting metadata query")
	response, err := tc.syncRequest(ctx, internal.NewMetadataQuery(stream))
	if err != nil {
		logger.Error(err, "error getting metadata", "stream", stream)
		return nil, err
	}

	return response.(*MetadataResponse), streamErrorOrNil(response.ResponseCode())
}

// NotifyPublish TODO: godocs
func (tc *Client) NotifyPublish(c chan *PublishConfirm) <-chan *PublishConfirm {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.confirmsCh = c
	return c
}

// NotifyChunk is used to receive notifications about new chunks in the stream.
// Chunks are sent to the channel after a subscription. At this level the client does not
// track the subscriptionId. The subscriptionId is used to identify the consumer in the server.
// The channel is closed when the connection is closed.
func (tc *Client) NotifyChunk(c chan *Chunk) <-chan *Chunk {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.chunkCh = c
	return c
}

// NotifyCreditError TODO: go docs
func (tc *Client) NotifyCreditError(notification chan *CreditError) <-chan *CreditError {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.notifyCh = notification
	return notification
}

// StoreOffset sends the desired offset to the given stream with a given reference. No response is given
// by a RabbitMQ server to this request.
// https://github.com/rabbitmq/rabbitmq-server/blob/main/deps/rabbitmq_stream/docs/PROTOCOL.adoc#queryoffset
// To query the offset for a stream, see "QueryOffset"
// https://github.com/rabbitmq/rabbitmq-server/blob/main/deps/rabbitmq_stream/docs/PROTOCOL.adoc#queryoffset
func (tc *Client) StoreOffset(ctx context.Context, reference, stream string, offset uint64) error {
	if ctx == nil {
		return errNilContext
	}
	logger := logr.FromContextOrDiscard(ctx).WithName("StoreOffset")
	logger.V(debugLevel).Info("starting store offset", "reference", reference, "stream", stream)
	err := tc.request(ctx, internal.NewStoreOffsetRequest(reference, stream, offset))
	if err != nil {
		return err
	}

	return nil
}

// QueryOffset retrieves the last consumer offset stored for a given consumer Reference and stream.
// Useful for as consumer wants to know the last stored offset.
// "no offset found"  error is returned if no offset is stored for the given consumer Reference and stream.
// Zero (0) is a valid offset.
func (tc *Client) QueryOffset(ctx context.Context, reference string, stream string) (uint64, error) {
	if ctx == nil {
		return 0, errNilContext
	}
	logger := logr.FromContextOrDiscard(ctx).WithName("QueryOffset")
	logger.V(debugLevel).Info("starting query offset", "reference", reference, "stream", stream)
	response, err := tc.syncRequest(ctx, internal.NewQueryOffsetRequest(reference, stream))
	if err != nil {
		logger.Error(err, "error sending sync request for query offset", "reference", reference, "stream", stream)
		return 0, err
	}
	var offsetResponse *internal.QueryOffsetResponse
	if reflect.TypeOf(response) != reflect.TypeOf(offsetResponse) {
		return 0, errors.New("response is not of type *internal.QueryOffsetResponse")
	}
	offsetResponse = response.(*internal.QueryOffsetResponse)
	return offsetResponse.Offset(), streamErrorOrNil(response.ResponseCode())
}

// StreamStats gets the current stats for a given stream name. A map of statistics is returned, in the format
// map[string]int64
func (tc *Client) StreamStats(ctx context.Context, stream string) (map[string]int64, error) {
	if ctx == nil {
		return nil, errNilContext
	}
	logger := logr.FromContextOrDiscard(ctx).WithName("StreamStats")
	logger.V(debugLevel).Info("starting stream stats", "stream", stream)
	streamStatsResponse, err := tc.syncRequest(ctx, internal.NewStreamStatsRequest(stream))
	if err != nil {
		logger.Error(err, "error sending sync request for stream stats", "stream", stream)
		return nil, err
	}
	return streamStatsResponse.(*StreamStatsResponse).Stats, streamErrorOrNil(streamStatsResponse.ResponseCode())
}
