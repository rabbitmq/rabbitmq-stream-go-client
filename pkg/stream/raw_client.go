package stream

import (
	"context"
	"errors"
	"github.com/go-logr/logr"
	"github.com/gsantomaggio/rabbitmq-stream-go-client/internal"
	"math"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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

type RawClient struct {
	mu sync.Mutex
	// this channel is used for correlation-less incoming frames from the server
	frameBodyListener    chan internal.CommandRead
	isOpen               bool
	connection           *internal.Connection
	correlationsMap      sync.Map
	nextCorrelation      uint32
	configuration        *RawClientConfiguration
	connectionProperties map[string]string
}

func (tc *RawClient) IsOpen() bool {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	return tc.isOpen
}

func NewRawClient(connection net.Conn, configuration *RawClientConfiguration) Clienter {
	rawClient := &RawClient{
		frameBodyListener: make(chan internal.CommandRead),
		connection:        internal.NewConnection(connection),
		isOpen:            false,
		correlationsMap:   sync.Map{},
		configuration:     configuration,
	}
	// TODO: check Dial function and use a default one if required
	return rawClient
}

// correlation map section

func (tc *RawClient) getCorrelationById(id uint32) *correlation {
	if v, ok := tc.correlationsMap.Load(id); ok {
		return v.(*correlation)
	}
	return nil
}

func (tc *RawClient) storeCorrelation(request internal.CommandWrite) {
	request.SetCorrelationId(tc.getNextCorrelation())
	tc.correlationsMap.Store(request.CorrelationId(), newCorrelation(request.CorrelationId()))
}

func (tc *RawClient) removeCorrelation(id uint32) {
	tc.getCorrelationById(id).Close()
	tc.correlationsMap.Delete(id)
}

// end correlation map section

func (tc *RawClient) writeCommand(request internal.CommandWrite) error {
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

// This makes an RPC-style request. We send the frame and we await for a response
func (tc *RawClient) request(request internal.CommandWrite) (internal.CommandRead, error) {
	// TODO: refactor to use context.Context
	tc.storeCorrelation(request)
	defer tc.removeCorrelation(request.CorrelationId())
	err := tc.writeCommand(request)
	if err != nil {
		return nil, err
	}
	select {
	// TODO: add a case for ctx.Done()
	case r := <-tc.getCorrelationById(request.CorrelationId()).chResponse:
		return r, nil
	}
}

func (tc *RawClient) getNextCorrelation() uint32 {
	return atomic.AddUint32(&tc.nextCorrelation, 1)
}

func (tc *RawClient) handleResponse(read internal.CommandRead) {
	// fixme: potential nil pointer dereference and panics
	tc.getCorrelationById(read.CorrelationId()).chResponse <- read
}

func (tc *RawClient) handleIncoming(ctx context.Context) error {
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
				tc.handleResponse(peerPropResponse)
			case internal.CommandSaslHandshake:
				saslMechanismsResponse := internal.NewSaslHandshakeResponse()
				err = saslMechanismsResponse.Read(buffer)
				if err != nil {
					log.Error(err, "error decoding SASL handshake")
					return err
				}
				tc.handleResponse(saslMechanismsResponse)
			case internal.CommandSaslAuthenticate:
				saslAuthResp := new(internal.SaslAuthenticateResponse)
				err = saslAuthResp.Read(buffer)
				if err != nil {
					log.Error(err, "error decoding SASL authenticate")
					return err
				}
				tc.handleResponse(saslAuthResp)
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
				tc.handleResponse(openResp)
			case internal.CommandClose:
				// FIXME: we may receive the request from the server
				// 		in such case, we have to start the shutdown process
				// 		we should stop decoding they key ID
				closeResp := new(internal.CloseResponse)
				err = closeResp.Read(buffer)
				if err != nil {
					log.Error(err, "error decoding open")
					return err
				}
				tc.handleResponse(closeResp)
			default:
				log.Info("frame not implemented", "command ID", header.Command())
			}
		}
	}
	//return nil
}

func (tc *RawClient) peerProperties(ctx context.Context) error {
	if ctx == nil {
		return errNilContext
	}
	log := logr.FromContextOrDiscard(ctx).WithName("peer properties")

	serverPropertiesResponse, err := tc.request(internal.NewPeerPropertiesRequest())
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

func (tc *RawClient) saslHandshake(ctx context.Context) error {
	if ctx == nil {
		return errNilContext
	}
	log := logr.FromContextOrDiscard(ctx).WithName("sasl handshake")
	saslMechanisms, err := tc.request(internal.NewSaslHandshakeRequest())
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

func (tc *RawClient) saslAuthenticate(ctx context.Context) error {
	// FIXME: make this pluggable to allow different authentication backends
	log := logr.FromContextOrDiscard(ctx).WithName("sasl authenticate")
	for _, mechanism := range tc.configuration.authMechanism {
		if strings.EqualFold(mechanism, "PLAIN") {
			log.V(debugLevel).Info("found PLAIN mechanism as supported")
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

			_, err = tc.request(saslAuthReq)
			if err != nil {
				log.Error(err, "error in SASL authenticate request")
				return err
			}
			//if saslAuthResp.ResponseCode() != internal.ResponseCodeOK {
			//	errCode, ok := streamErrors[saslAuthResp.ResponseCode()]
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

func (tc *RawClient) open(ctx context.Context, brokerIndex int) error {
	if ctx == nil {
		return errNilContext
	}
	log := logr.FromContextOrDiscard(ctx).WithName("open")
	rabbit := tc.configuration.rabbitmqBrokers[brokerIndex]
	openReq := internal.NewOpenRequest(rabbit.Vhost)
	openRespCommand, err := tc.request(openReq)
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

// TODO go docs
func DialConfig(ctx context.Context, config *RawClientConfiguration) (Clienter, error) {
	// FIXME: test this code path at system level
	// FIXME: try to test this with net.Pipe fake
	//		dialer should return the fakeClientConn from net.Pipe
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

	client := NewRawClient(conn, config)
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
		// A deadline is set for TLS and AMQP handshaking. After AMQP is established,
		// the deadline is cleared in openComplete.
		if err := conn.SetDeadline(time.Now().Add(connectionTimeout)); err != nil {
			return nil, err
		}

		return conn, nil
	}
}

// TODO go docs
func (tc *RawClient) Connect(ctx context.Context) error {
	if ctx == nil {
		return errNilContext
	}

	logger := logr.FromContextOrDiscard(ctx).WithName("connect")
	logger.Info("starting connection")

	var i = 0 // broker index for chosen broker to dial to

	go func(ctx context.Context) {
		log := logr.FromContextOrDiscard(ctx)
		log.V(debugLevel).Info("starting frame listener")
		err := tc.handleIncoming(ctx)
		if err != nil {
			// FIXME: handle error, possibly shutdown or reconnect
			log.Error(err, "error handling incoming frames")
		}
	}(ctx)

	err := tc.peerProperties(ctx)
	if err != nil {
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

func (tc *RawClient) DeclareStream(ctx context.Context, name string) error {
	//TODO implement me
	panic("implement me")
}

func (tc *RawClient) Close(ctx context.Context) error {
	if ctx == nil {
		return errNilContext
	}

	log := logr.FromContextOrDiscard(ctx).WithName("close")
	log.V(debugLevel).Info("starting connection close")

	response, err := tc.request(internal.NewCloseRequest(internal.ResponseCodeOK, "kthxbye"))
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
