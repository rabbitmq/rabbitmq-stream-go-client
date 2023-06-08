package raw

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/gsantomaggio/rabbitmq-stream-go-client/internal"
	"github.com/gsantomaggio/rabbitmq-stream-go-client/pkg/common"
	"github.com/gsantomaggio/rabbitmq-stream-go-client/pkg/constants"
	"golang.org/x/exp/slog"
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
	frameBodyListener chan internal.SyncCommandRead
	// this function is used during shutdown to stop the background loop that reads from the network
	ioLoopCancelFn       context.CancelFunc
	connection           *internal.Connection
	correlationsMap      sync.Map
	nextCorrelation      uint32
	configuration        *ClientConfiguration
	connectionProperties map[string]string
	confirmsCh           chan *PublishConfirm
	publishErrorCh       chan *PublishError
	chunkCh              chan *Chunk
	notifyCh             chan *CreditError

	// see constants states of the connection
	// we need different states to handle the case where the connection is closed by the server
	// Open/ConnectionClosing/Closed
	// ConnectionClosing is used to set the connection status no longer open, but still waiting for the server to close the connection
	connectionStatus uint8
	// socketClosedCh is used to notify the client that the socket has been closed
	socketClosedCh   chan error
	metadataUpdateCh chan *MetadataUpdate
	consumerUpdateCh chan *ConsumerUpdate
	heartbeatCh      chan *Heartbeat
}

// IsOpen returns true if the connection is open, false otherwise
// IsOpen is thread-safe
func (tc *Client) IsOpen() bool {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	return tc.connectionStatus == constants.ConnectionOpen
}

// NewClient returns a common.Clienter implementation to interact with RabbitMQ stream using low level primitives.
// NewClient requires an established net.Conn and a ClientConfiguration. Using DialConfig() is the preferred method
// to establish a connection to RabbitMQ servers.
func NewClient(connection net.Conn, configuration *ClientConfiguration) Clienter {
	rawClient := &Client{
		frameBodyListener: make(chan internal.SyncCommandRead),
		connection:        internal.NewConnection(connection),
		connectionStatus:  constants.ConnectionClosed,
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
	logger := LoggerFromCtxOrDiscard(ctx).WithGroup("correlation-map")
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

	logger := LoggerFromCtxOrDiscard(ctx).WithGroup("syncRequest")

	tc.storeCorrelation(request)
	defer tc.removeCorrelation(ctx, request.CorrelationId())

	logger.Debug("writing sync command to the wire", "syncRequest", request)
	err := internal.WriteCommandWithHeader(request, tc.connection.GetWriter())
	if err != nil {
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

	logger := LoggerFromCtxOrDiscard(ctx)
	// FIXME: Perhaps too verbose. May need to exclude the request, and log only the request command ID
	logger.Debug("writing command to the wire", "request", request)
	return internal.WriteCommandWithHeader(request, tc.connection.GetWriter())
}

// handleResponse is responsible for handling incoming frames from the server.
// in this case the call is synchronous, so we need to correlate the response with the request
// and send it to the right channel that is waiting for it.
func (tc *Client) handleResponse(ctx context.Context, read internal.SyncCommandRead) {
	var logger *slog.Logger
	if ctx != nil {
		logger = LoggerFromCtxOrDiscard(ctx)
	} else {
		logger = NewDiscardLogger()
	}

	correlation := tc.getCorrelationById(read.CorrelationId())
	if correlation == nil {
		logger.Debug(
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

	log := LoggerFromCtxOrDiscard(ctx).WithGroup("handleIncoming")
	buffer := tc.connection.GetReader()
	for {
		select {
		case <-ctx.Done():
			log.Info("context cancelled", "reason", ctx.Err())
			return ctx.Err()
		default:
			// TODO: use a pool of headers to avoid allocation on each iteration
			var header = new(internal.Header)
			// TODO: set an I/O deadline to avoid deadlock on I/O
			// 		renew the deadline at the beginning of each iteration
			// 		clear the deadline after reading the header
			err := header.Read(buffer)
			if errors.Is(err, io.ErrClosedPipe) {
				return nil
			}

			if errors.Is(err, io.EOF) {
				// EOF is returned when the connection is closed
				if tc.socketClosedCh != nil {
					if tc.IsOpen() {
						tc.socketClosedCh <- ErrConnectionClosed
					}
					// the TCP connection here is closed
					// we close the channel since we don't need to send more than one message
					close(tc.socketClosedCh)
					tc.socketClosedCh = nil
				}
				// set the shutdown flag to false since we don't want to close the connection
				// since it's already closed
				_ = tc.shutdown(false)
				return nil
			}

			if err != nil {
				// TODO: some errors may be recoverable. We only need to return if reconnection
				// 	is needed
				return err
			}
			switch header.Command() {
			case internal.CommandPeerPropertiesResponse:
				peerPropResponse := internal.NewPeerPropertiesResponse()
				err = peerPropResponse.Read(buffer)
				if err != nil {
					return err
				}
				tc.handleResponse(ctx, peerPropResponse)
			case internal.CommandSaslHandshakeResponse:
				saslMechanismsResponse := internal.NewSaslHandshakeResponse()
				err = saslMechanismsResponse.Read(buffer)
				if err != nil {
					return err
				}
				tc.handleResponse(ctx, saslMechanismsResponse)
			case internal.CommandSaslAuthenticateResponse:
				saslAuthResp := new(internal.SaslAuthenticateResponse)
				err = saslAuthResp.Read(buffer)
				if err != nil {
					return err
				}
				tc.handleResponse(ctx, saslAuthResp)
			case internal.CommandTune:
				tuneReq := new(internal.TuneRequest)
				err = tuneReq.Read(buffer)
				if err != nil {
					return err
				}
				tc.frameBodyListener <- tuneReq
			case internal.CommandOpenResponse:
				openResp := new(internal.OpenResponse)
				err = openResp.Read(buffer)
				if err != nil {
					return err
				}
				tc.handleResponse(ctx, openResp)
			case internal.CommandClose:
				// server closed the connection
				closeReq := new(internal.CloseRequest)
				err = closeReq.Read(buffer)
				if err != nil {
					return err
				}

				log.Info("server requested connection close", "close-reason", closeReq.ClosingReason())
				err = tc.handleClose(ctx, closeReq)
				if err != nil {
					log.Error("error sending close response", "error", err)
					// we do not return here because we must execute the shutdown process and close the socket
				}

				err = tc.shutdown(true)
				if err != nil && !errors.Is(err, io.ErrClosedPipe) {
					return err
				}
				return nil
			case internal.CommandCreateResponse,
				internal.CommandDeleteResponse,
				internal.CommandDeclarePublisherResponse,
				internal.CommandDeletePublisherResponse,
				internal.CommandCloseResponse,
				internal.CommandSubscribeResponse,
				internal.CommandUnsubscribeResponse:
				createResp := new(internal.SimpleResponse)
				err = createResp.Read(buffer)
				if err != nil {
					return err
				}
				tc.handleResponse(ctx, createResp)
			case internal.CommandPublishConfirm:
				publishConfirm := new(internal.PublishConfirmResponse)
				err = publishConfirm.Read(buffer)
				if err != nil {
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
					log.Debug("sent a publish confirm", "publisherId", publishConfirm.PublisherID())
				}
			case internal.CommandPublishError:
				publishError := new(internal.PublishErrorResponse)
				err = publishError.Read(buffer)
				if err != nil {
					return err
				}
				if tc.publishErrorCh == nil {
					log.Info("error channel is not registered. Use Client.NotifyPublishError() to receive publish errors")
					continue
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case tc.publishErrorCh <- publishError:
					log.Debug("sent a publish error",
						"publisherId", publishError.PublisherId(),
						"publishingId", publishError.PublishingId(),
						"code", publishError.Code(),
					)
				}
			case internal.CommandMetadataUpdate:
				metadataUpdate := new(internal.MetadataUpdateResponse)
				err = metadataUpdate.Read(buffer)
				if err != nil {
					return err
				}

				if tc.metadataUpdateCh == nil {
					log.Info("metadata channel is not registered. Use Client.NotifyMetadata() to receive metadata updates")
					continue
				}

				select {
				case <-ctx.Done():
					return ctx.Err()
				case tc.metadataUpdateCh <- metadataUpdate:
					log.Debug("sent a metadata update for stream",
						"code", metadataUpdate.Code(),
						"stream", metadataUpdate.Stream())
				}
			case internal.CommandHeartbeat:
				hb := new(internal.Heartbeat)
				err = hb.Read(buffer)
				if err != nil {
					return err
				}
				if tc.heartbeatCh == nil {
					log.Info("heartbeat channel is not registered. Use Client.NotifyHeartbeat() to receive heartbeats")
					continue
				}

				select {
				case <-ctx.Done():
					return ctx.Err()
				case tc.heartbeatCh <- hb:
					log.Debug("heartbeat received")
				}
			case internal.CommandDeliver:
				chunkResponse := new(internal.ChunkResponse)
				err = chunkResponse.Read(buffer)
				log.WithGroup("deliver").
					Debug(
						"received a chunk",
						slog.Uint64("chunkId", chunkResponse.CommittedChunkId),
						slog.Int64("chunkTimestamp", chunkResponse.Timestamp),
						slog.Any("subscriptionId", chunkResponse.SubscriptionId),
					)
				if err != nil {
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
				}
			case internal.CommandConsumerUpdateQuery:
				consumerUpdate := new(internal.ConsumerUpdateQuery)
				err = consumerUpdate.Read(buffer)
				log.Debug("received consumer update query")
				if err != nil {
					return err
				}
				if tc.consumerUpdateCh == nil {
					log.Info("consumer update channel is not registered. Use Client.NotifyConsumerUpdate() to receive updates")
					continue
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case tc.consumerUpdateCh <- consumerUpdate:
					log.Debug("received a consumer update", "subscriptionId", consumerUpdate.SubscriptionId())
				}
			case internal.CommandExchangeCommandVersionsResponse:
				exchangeResponse := new(internal.ExchangeCommandVersionsResponse)
				err = exchangeResponse.Read(buffer)
				log.Debug("received exchange command versions response")
				if err != nil {
					log.Error("error exchanging command versions", "error", err)
				}
				tc.handleResponse(ctx, exchangeResponse)
			case internal.CommandQueryOffsetResponse:
				queryOffsetResponse := new(internal.QueryOffsetResponse)
				err = queryOffsetResponse.Read(buffer)
				log.Debug("received query offset response")
				if err != nil {
					log.Error("error receiving query offset response", "error", err)
				}
				// TODO: check for nil queryOffsetResponse?
				tc.handleResponse(ctx, queryOffsetResponse)
			case internal.CommandCreditResponse:
				// We only receive a response in there's an error
				creditResp := new(CreditError)
				err = creditResp.Read(buffer)
				log.Error(
					"received credit response for unknown subscription",
					"error", errUnknownSubscription,
					"responseCode", creditResp.ResponseCode(),
					"subscriptionId", creditResp.SubscriptionId(),
				)
				if err != nil {
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
				log.Debug("received metadata response")
				if err != nil {
					log.Error("error in metadata response", "error", err)
				}
				// TODO: should we check for nil in metadataResp? Or return in the above if?
				tc.handleResponse(ctx, metadataResp)
			case internal.CommandStreamStatsResponse:
				streamStatsResp := new(internal.StreamStatsResponse)
				err := streamStatsResp.Read(buffer)
				log.Debug("received stream stats response")
				if err != nil {
					log.Error("error in stream stats response", "error", err)
				}
				tc.handleResponse(ctx, streamStatsResp)
			case internal.CommandQueryPublisherSequenceResponse:
				queryPublisherSequenceResp := new(internal.QueryPublisherSequenceResponse)
				err := queryPublisherSequenceResp.Read(buffer)
				log.Debug("received publisher sequence response")
				if err != nil {
					log.Error("error in publisher sequence response", "error", err)
				}
				tc.handleResponse(ctx, queryPublisherSequenceResp)
			case internal.CommandRouteResponse:
				routeResp := new(internal.RouteResponse)
				err := routeResp.Read(buffer)
				log.Debug("received route response")
				if err != nil {
					log.Error("error in route response", "error", err)
				}
				tc.handleResponse(ctx, routeResp)
			case internal.CommandPartitionsResponse:
				partitionsResp := new(internal.PartitionsResponse)
				err := partitionsResp.Read(buffer)
				log.Debug("received stream stats response")
				if err != nil {
					log.Error("error in stream stats response", "error", err)
				}
				tc.handleResponse(ctx, partitionsResp)
			default:
				log.Info("frame not implemented", "command ID", fmt.Sprintf("%X", header.Command()))
				_, err := buffer.Discard(header.Length() - 4)
				if err != nil {
					log.Debug("error discarding bytes from unknown frame", "error", err, "discard", header.Length()-4)
				}
			}
		}
	}
}

func (tc *Client) peerProperties(ctx context.Context) error {
	if ctx == nil {
		return errNilContext
	}

	log := LoggerFromCtxOrDiscard(ctx).WithGroup("peer properties")
	log.Debug("starting peer properties")
	serverPropertiesResponse, err := tc.syncRequest(ctx,
		internal.NewPeerPropertiesRequest(tc.configuration.connectionName))
	if err != nil {
		return err
	}
	response, ok := serverPropertiesResponse.(*internal.PeerPropertiesResponse)
	if !ok {
		panic("could not polymorph response")
	}
	log.Debug(
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
	log := LoggerFromCtxOrDiscard(ctx).WithGroup("sasl handshake")
	log.Debug("starting SASL handshake")

	saslMechanisms, err := tc.syncRequest(ctx, internal.NewSaslHandshakeRequest())
	saslMechanismResponse, ok := saslMechanisms.(*internal.SaslHandshakeResponse)
	if !ok {
		panic("could not polymorph response")
	}
	log.Debug(
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
	log := LoggerFromCtxOrDiscard(ctx).WithGroup("sasl authenticate")
	log.Debug("starting SASL authenticate")

	for _, mechanism := range tc.configuration.authMechanism {
		if strings.EqualFold(mechanism, "PLAIN") {
			log.Debug("found PLAIN mechanism as supported")
			saslPlain := internal.NewSaslPlainMechanism(
				tc.configuration.rabbitmqBroker.Username,
				tc.configuration.rabbitmqBroker.Password,
			)
			saslAuthReq := internal.NewSaslAuthenticateRequest(mechanism)
			err := saslAuthReq.SetChallengeResponse(saslPlain)
			if err != nil {
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

func (tc *Client) open(ctx context.Context) error {
	if ctx == nil {
		return errNilContext
	}
	log := LoggerFromCtxOrDiscard(ctx).WithGroup("open")
	log.Debug("starting open")

	rabbit := tc.configuration.rabbitmqBroker
	openReq := internal.NewOpenRequest(rabbit.Vhost)
	openRespCommand, err := tc.syncRequest(ctx, openReq)
	if err != nil {
		return err
	}

	openResp, ok := openRespCommand.(*internal.OpenResponse)
	if !ok {
		panic("could not polymorph response")
	}
	log.Debug(
		"open syncRequest success",
		"connection properties",
		openResp.ConnectionProperties(),
	)
	tc.connectionProperties = openResp.ConnectionProperties()

	return streamErrorOrNil(openResp.ResponseCode())
}

func (tc *Client) shutdown(closeConnection bool) error {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	// The method can be called by the Close() method or by the
	// connection error handler, see: handleIncoming EOF error
	// the shutdown method has to be idempotent since it can be called multiple times
	// In case of unexpected connection error, the shutdown is called just once
	tc.connectionStatus = constants.ConnectionClosed
	tc.ioLoopCancelFn()

	if tc.confirmsCh != nil {
		close(tc.confirmsCh)
		tc.confirmsCh = nil
	}

	if tc.chunkCh != nil {
		close(tc.chunkCh)
		tc.chunkCh = nil
	}

	if tc.socketClosedCh != nil {
		close(tc.socketClosedCh)
		tc.socketClosedCh = nil
	}

	// if the caller is handleIncoming EOF error closeConnection is false,
	// The connection is already closed
	// So we don't need to close it again

	if closeConnection {
		return tc.connection.Close()
	}
	return nil
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
	if n != 14 { // simple response is always 10 bytes + 4 bytes for frame len
		return errWriteShort
	}
	return tc.connection.GetWriter().Flush()
}

// public API

// DialConfig establishes a connection to RabbitMQ servers in
// common.Configuration. It returns an error if the connection cannot be
// established. On a successful connection, in returns an implementation of
// common.Clienter, capable of interacting to RabbitMQ streams binary protocol at
// a low level.
//
// This is the recommended method to connect to RabbitMQ. After this function
// returns, a connection is established and authenticated with RabbitMQ. Do NOT
// call Client.Connect() after this function.
//
// ClientConfiguration must not be nil. ClientConfiguration should be initialised
// using NewClientConfiguration(). A custom dial function can be set using
// ClientConfiguration.SetDial(). Check ClientConfiguration.SetDial() for more
// information. If dial function is not provided, DefaultDial is used with a
// timeout of 30 seconds. DefaultDial uses net.Dial
//
// If you want to implement a custom timeout or dial-retry mechanism, provide
// your own dial function using [raw.ClientConfiguration] SetDial(). A simple
// dial function could be as follows:
//
//	cfg.SetDial(func(network, addr string) (net.Conn, error) {
//		return net.DialTimeout(network, addr, time.Second)
//	})
func DialConfig(ctx context.Context, config *ClientConfiguration) (Clienter, error) {
	// FIXME: try to test this with net.Pipe fake
	//		dialer should return the fakeClientConn from net.Pipe
	if config == nil {
		return nil, errNilConfig
	}
	if ctx == nil {
		return nil, errNilContext
	}
	log := LoggerFromCtxOrDiscard(ctx).WithGroup("dial")
	var err error
	var conn net.Conn

	dialer := config.dial
	if dialer == nil {
		log.Debug("no dial function provided, using default Dial")
		dialer = DefaultDial(defaultConnectionTimeout)
	}

	select {
	default:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	// TODO: TLS if scheme is rabbitmq-stream+tls
	addr := net.JoinHostPort(config.rabbitmqBroker.Host, strconv.FormatInt(int64(config.rabbitmqBroker.Port), 10))
	conn, err = dialer("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to dial RabbitMQ '%s': %w", addr, err)
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

	log := LoggerFromCtxOrDiscard(ctx).WithGroup("connect")
	log.Info("starting connection")

	// We have to create a new context here and keep a reference to the cancelling function.
	// This context controls the i/o loop that reads from the network socket, and it is
	// cancelled after calling `Client.Close()`.
	// We can't use the ctx passed as parameter because that context is tied to the lifecycle
	// of this function call. In other words, the context may have a deadline, representing a timeout
	// to execute Connect(). The i/o loop in handleIncoming() must not be tight to the same deadline.
	//
	// https://github.com/Gsantomaggio/rabbitmq-stream-go-client/issues/27
	ioLoopCtx, cancel := context.WithCancel(context.Background())
	tc.ioLoopCancelFn = cancel
	go func(ctx context.Context) {
		log := LoggerFromCtxOrDiscard(ctx).WithGroup("frame-listener")
		log.Debug("starting frame listener")

		select {
		default:
		case <-ctx.Done():
			log.Info("context cancelled", "err", ctx.Err())
			return
		}

		err := tc.handleIncoming(ctx)
		if err != nil {
			log.Error("error handling incoming frames", "error", err)
		}
	}(ioLoopCtx)

	err := tc.peerProperties(ctx)
	if err != nil {
		// FIXME: wrap error in Connect-specific error
		return err
	}
	err = tc.saslHandshake(ctx)
	if err != nil {
		return err
	}

	err = tc.saslAuthenticate(ctx)
	if err != nil {
		return err
	}

	log.Debug("awaiting Tune frame from server")
	tuneCtx, tuneCancel := context.WithTimeout(ctx, time.Second*30)
	select {
	case <-tuneCtx.Done():
		tuneCancel()
		return fmt.Errorf("error awaiting for tune from server: %w", tuneCtx.Err())
	case tuneReqCommand := <-tc.frameBodyListener:
		tuneReq, ok := tuneReqCommand.(*internal.TuneRequest)
		if !ok {
			panic("could not polymorph SyncCommandRead into TuneRequest")
		}

		desiredFrameSize := math.Min(float64(tuneReq.FrameMaxSize()), float64(tc.configuration.clientMaxFrameSize))
		desiredHeartbeat := math.Min(float64(tuneReq.HeartbeatPeriod()), float64(tc.configuration.clientHeartbeat))

		log.Debug(
			"desired tune options",
			"frame-size",
			desiredFrameSize,
			"heartbeat",
			desiredHeartbeat,
		)
		tuneResp := internal.NewTuneResponse(uint32(desiredFrameSize), uint32(desiredHeartbeat))
		err = internal.WriteCommandWithHeader(tuneResp, tc.connection.GetWriter())
		if err != nil {
			tuneCancel()
			return fmt.Errorf("error in Tune: %w", err)
		}
	}
	tuneCancel()

	err = tc.open(ctx)
	if err != nil {
		return fmt.Errorf("error in open: %w", err)
	}

	// clear any deadline set by Dial functions.
	// Dial functions may set an i/o timeout for TLS and AMQP handshake.
	// Such timeout should not apply to stream i/o operations
	log.Debug("clearing connection I/O deadline")
	err = tc.connection.SetDeadline(time.Time{})
	if err != nil {
		return err
	}

	tc.mu.Lock()
	tc.connectionStatus = constants.ConnectionOpen
	defer tc.mu.Unlock()
	log.Info("connection is open")

	return nil
}

// DeclareStream sends a syncRequest to create a new Stream. If the error is nil, the
// Stream was created successfully, and it is ready to use.
// By default, the stream is created without any retention policy, so stream can grow indefinitely.
// It is recommended to set a retention policy to avoid filling up the disk.
// See also https://www.rabbitmq.com/streams.html#retention
func (tc *Client) DeclareStream(ctx context.Context, stream string, configuration StreamConfiguration) error {
	if ctx == nil {
		return errNilContext
	}

	log := LoggerFromCtxOrDiscard(ctx).WithGroup("declareStream")
	log.Debug("starting declare stream", "stream", stream)

	createResponse, err := tc.syncRequest(ctx, internal.NewCreateRequest(stream, configuration))
	if err != nil {
		return &DeclareStreamError{stream, configuration, err}
	}
	return streamErrorOrNil(createResponse.ResponseCode())
}

// DeleteStream sends a syncRequest to delete a Stream. If the error is nil, the
// Stream was deleted successfully.
func (tc *Client) DeleteStream(ctx context.Context, stream string) error {
	if ctx == nil {
		return errNilContext
	}

	log := LoggerFromCtxOrDiscard(ctx).WithGroup("deleteStream")
	log.Debug("starting delete stream", "stream", stream)

	deleteResponse, err := tc.syncRequest(ctx, internal.NewDeleteRequest(stream))
	if err != nil {
		return &DeleteStreamError{stream, err}
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

	log := LoggerFromCtxOrDiscard(ctx).WithGroup("declarePublisher")
	log.Debug("starting declare publisher", "publisherId", publisherId, "publisherReference", publisherReference, "stream", stream)

	deleteResponse, err := tc.syncRequest(ctx, internal.NewDeclarePublisherRequest(publisherId, publisherReference, stream))
	if err != nil {
		return &DeclarePublisherError{publisherId, publisherReference, stream, err}
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

	logger := LoggerFromCtxOrDiscard(ctx).WithGroup("send")
	logger.Debug("starting send", "publisherId", publisherId, "message-count", len(publishingMessages))

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
	publishingId uint64, compress common.CompresserCodec, messages []common.Message) error {
	if ctx == nil {
		return errNilContext
	}
	logger := LoggerFromCtxOrDiscard(ctx).WithGroup("SendSubEntryBatch")
	logger.Debug("starting send-sub-entry-batch", "publisherId", publisherId, "messageCount", len(messages))

	buff := new(bytes.Buffer)
	for _, msg := range messages {
		binary, err := msg.MarshalBinary()
		if err != nil {
			return err
		}
		_, err = internal.WriteMany(buff, len(binary), binary)
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
		uint16(len(messages)),
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

	log := LoggerFromCtxOrDiscard(ctx).WithGroup("deletePublisher")
	log.Debug("starting delete publisher", "publisherId", publisherId)

	deleteResponse, err := tc.syncRequest(ctx, internal.NewDeletePublisherRequest(publisherId))
	if err != nil {
		return &DeletePublisherError{publisherId, err}
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

	log := LoggerFromCtxOrDiscard(ctx).WithGroup("subscribe")
	log.Debug("starting subscribe consumer",
		"subscriptionId", subscriptionId,
		"stream", stream,
		"offsetType", offsetType,
		"offset", offset,
		"credit", credit,
		"properties", properties)

	subscribeResponse, err := tc.syncRequest(ctx, internal.NewSubscribeRequestRequest(subscriptionId, stream, offsetType, offset, credit, properties))
	if err != nil {
		return &SubscribeError{subscriptionId, stream, offsetType, offset, credit, properties, err}
	}
	return streamErrorOrNil(subscribeResponse.ResponseCode())
}

// Unsubscribe sends a sync request with a subscriptionId, to identify a consumer on the
// server, and unsubscribe.
func (tc *Client) Unsubscribe(ctx context.Context, subscriptionId uint8) error {
	if ctx == nil {
		return errNilContext
	}
	unSubscribeResponse, err := tc.syncRequest(ctx, internal.NewUnsubscribeRequest(subscriptionId))
	if err != nil {
		return err
	}
	// FIXME: close subscription channel
	return streamErrorOrNil(unSubscribeResponse.ResponseCode())
}

// Close gracefully shutdowns the connection to RabbitMQ. The Client will send a
// close request to RabbitMQ, and it will await a response. It is recommended to
// set a deadline in the context, to avoid waiting forever on a non-responding
// RabbitMQ server.
//
// Regardless of the returned error of this function, the client should be considered
// closed and must not be used after calling this function.
func (tc *Client) Close(ctx context.Context) error {
	if ctx == nil {
		return errNilContext
	}

	tc.connectionStatus = constants.ConnectionClosing

	log := LoggerFromCtxOrDiscard(ctx).WithGroup("close")
	log.Debug("starting connection close")

	response, err := tc.syncRequest(ctx, internal.NewCloseRequest(constants.ResponseCodeOK, "kthxbye"))
	if err != nil {
		return err
	}

	err = streamErrorOrNil(response.ResponseCode())
	log.Debug("server response", "response code", response.ResponseCode())
	if err != nil {
		log.Error("close response code is not OK", "error", err)
	}

	err = tc.shutdown(true)
	if err != nil && !errors.Is(err, io.ErrClosedPipe) {
		return err
	}
	return nil
}

// ExchangeCommandVersions TODO: godocs
func (tc *Client) ExchangeCommandVersions(ctx context.Context) error {
	if ctx == nil {
		return errNilContext
	}

	logger := LoggerFromCtxOrDiscard(ctx).WithGroup("exchangeCommandVersions")
	logger.Debug("starting exchange command versions")
	response, err := tc.syncRequest(ctx, internal.NewExchangeCommandVersionsRequest())
	if err != nil {
		return err
	}

	return streamErrorOrNil(response.ResponseCode())
}

// Credit TODO: go docs
func (tc *Client) Credit(ctx context.Context, subscriptionID uint8, credits uint16) error {
	if ctx == nil {
		return errNilContext
	}
	logger := LoggerFromCtxOrDiscard(ctx).WithGroup("credit")
	logger.Debug("starting credit")

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

	logger := LoggerFromCtxOrDiscard(ctx).WithGroup("metadataQuery")
	logger.Debug("starting metadata query")
	response, err := tc.syncRequest(ctx, internal.NewMetadataQuery(stream))
	if err != nil {
		return nil, &MetadataQueryError{stream, err}
	}

	return response.(*MetadataResponse), streamErrorOrNil(response.ResponseCode())
}

// NotifyMetadata receives notifications about updates to metadata for a stream.
// Updates are sent to the channel when they occur. The channel is closed when
// the connection is closed
func (tc *Client) NotifyMetadata() <-chan *MetadataUpdate {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	m := make(chan *MetadataUpdate, 1)
	tc.metadataUpdateCh = m
	return m
}

// NotifyPublish TODO: godocs
func (tc *Client) NotifyPublish(c chan *PublishConfirm) <-chan *PublishConfirm {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.confirmsCh = c
	return c
}

// NotifyPublishError receives notifications about publishing errors. The publishingID
// and code is returned. The channel is closed when the connection is closed
func (tc *Client) NotifyPublishError() <-chan *PublishError {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	p := make(chan *PublishError, 1)
	tc.publishErrorCh = p
	return p
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

// NotifyConnectionClosed receives notifications about connection closed events.
// It is raised only once when the connection is closed in unexpected way.
// Connection gracefully closed by the client will not raise this event.
func (tc *Client) NotifyConnectionClosed() <-chan error {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	// The user can't decide the size of the channel, so we use a buffer of 1.
	// NotifyConnectionClosed is one shot notification, so we don't need a buffer.
	// buffer greater than 1 cloud cause a deadlock since the channel is closed after the first notification.
	c := make(chan error, 1)
	tc.socketClosedCh = c
	return c
}

// NotifyCreditError TODO: go docs
func (tc *Client) NotifyCreditError(notification chan *CreditError) <-chan *CreditError {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.notifyCh = notification
	return notification
}

// NotifyConsumerUpdate is used to receive activity updates for consumers connected to a stream
// in a single-active consumer scenario.
func (tc *Client) NotifyConsumerUpdate() <-chan *ConsumerUpdate {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	c := make(chan *ConsumerUpdate, 1)
	tc.consumerUpdateCh = c
	return c
}

// NotifyHeartbeat is used to detect when heartbeats are received from the RabbitMQ server.
func (tc *Client) NotifyHeartbeat() <-chan *Heartbeat {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	hb := make(chan *Heartbeat, 1)
	tc.heartbeatCh = hb
	return hb
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
	logger := LoggerFromCtxOrDiscard(ctx).WithGroup("StoreOffset")
	logger.Debug("starting store offset", "reference", reference, "stream", stream)
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
	logger := LoggerFromCtxOrDiscard(ctx).WithGroup("queryOffset")
	logger.Debug("starting query offset", "reference", reference, "stream", stream)
	response, err := tc.syncRequest(ctx, internal.NewQueryOffsetRequest(reference, stream))
	if err != nil {
		return 0, &QueryOffsetError{reference, stream, err}
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
	logger := LoggerFromCtxOrDiscard(ctx).WithGroup("StreamStats")
	logger.Debug("starting stream stats", "stream", stream)
	streamStatsResponse, err := tc.syncRequest(ctx, internal.NewStreamStatsRequest(stream))
	if err != nil {
		return nil, &StreamStatsError{stream, err}
	}
	return streamStatsResponse.(*internal.StreamStatsResponse).Stats, streamErrorOrNil(streamStatsResponse.ResponseCode())
}

// QueryPublisherSequence returns the sequence for a given publisher reference and stream
func (tc *Client) QueryPublisherSequence(ctx context.Context, ref, stream string) (uint64, error) {
	if ctx == nil {
		return 0, errNilContext
	}
	queryPublisherSequence, err := tc.syncRequest(ctx, internal.NewQueryPublisherSequenceRequest(ref, stream))
	if err != nil {
		return 0, err
	}

	return queryPublisherSequence.(*internal.QueryPublisherSequenceResponse).Sequence(), nil
}

// RouteQuery retrieves the stream name for a given routing key and superStream. If a stream is found
// the name is returned as a string.
func (tc *Client) RouteQuery(ctx context.Context, routingKey, superStream string) ([]string, error) {
	if ctx == nil {
		return nil, errNilContext
	}
	response, err := tc.syncRequest(ctx, internal.NewRouteQuery(routingKey, superStream))
	if err != nil {
		return nil, err
	}
	var routeResponse *internal.RouteResponse
	if reflect.TypeOf(response) != reflect.TypeOf(routeResponse) {
		return nil, errors.New("response is not of type *internal.RouteResponse")
	}
	routeResponse = response.(*internal.RouteResponse)
	return routeResponse.Streams(), streamErrorOrNil(response.ResponseCode())
}

// Partitions returns the partition streams for a given superStream
func (tc *Client) Partitions(ctx context.Context, superStream string) ([]string, error) {
	if ctx == nil {
		return nil, errNilContext
	}
	partitionsResponse, err := tc.syncRequest(ctx, internal.NewPartitionsQuery(superStream))
	if err != nil {
		return nil, err
	}
	return partitionsResponse.(*internal.PartitionsResponse).Streams(), err
}

// ConsumerUpdateResponse sends a request to the broker in response to a ConsumerUpdateQuery. The user must
// calculate the required offset and offsetType.
func (tc *Client) ConsumerUpdateResponse(ctx context.Context, correlationId uint32, responseCode uint16, offsetType uint16, offset uint64) error {
	if ctx == nil {
		return errNilContext
	}

	err := tc.request(ctx, internal.NewConsumerUpdateResponse(correlationId, responseCode, offsetType, offset))
	if err != nil {
		return err
	}

	return err
}

// SendHeartbeat sends heartbeat frames to the server. We don't wait for a response. Context should have a deadline
// or timeout to avoid deadlock scenario if RabbitMQ server is unresponsive.
func (tc *Client) SendHeartbeat() error {
	return internal.WriteCommand(internal.NewHeartbeat(), tc.connection.GetWriter())
}
