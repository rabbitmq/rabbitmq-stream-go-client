package stream

import (
	"context"
	"crypto/tls"
	"errors"
	"github.com/gsantomaggio/rabbitmq-stream-go-client/internal"
	"net"
	"time"
)

var (
	errURIScheme     = errors.New("RabbitMQ Stream scheme must be either 'rabbitmq-stream://' or 'rabbitmq-stream+tls://'")
	errURIWhitespace = errors.New("URI must not contain whitespace")
	errNilContext    = errors.New("context cannot be nil")

	streamErrors = map[uint16]error{
		internal.ResponseCodeStreamDoesNotExist:          errors.New("stream does not exist"),
		internal.ResponseCodeSubscriptionIdAlreadyExists: errors.New("subscription ID already exists"),
		internal.ResponseCodeSubscriptionIdDoesNotExist:  errors.New("subscription ID does not exist"),
		internal.ResponseCodeStreamAlreadyExists:         errors.New("stream already exists"),
		internal.ResponseCodeStreamNotAvailable:          errors.New("stream not available"),
		internal.ResponseCodeSASLMechanismNotSupported:   errors.New("SASL mechanism not supported"),
		internal.ResponseCodeAuthFailure:                 errors.New("authentication failure"),
		internal.ResponseCodeSASLError:                   errors.New("SASL error"),
		internal.ResponseCodeSASLChallenge:               errors.New("SASL challenge"),
		internal.ResponseCodeSASLAuthFailureLoopback:     errors.New("SASL authentication failure loopback"),
		internal.ResponseCodeVirtualHostAccessFailure:    errors.New("virtual host access failure"),
		internal.ResponseCodeUnknownFrame:                errors.New("unknown frame"),
		internal.ResponseCodeFrameTooLarge:               errors.New("frame too large"),
		internal.ResponseCodeInternalError:               errors.New("internal error"),
		internal.ResponseCodeAccessRefused:               errors.New("access refused"),
		internal.ResponseCodePreconditionFailed:          errors.New("precondition failed"),
		internal.ResponseCodePublisherDoesNotExist:       errors.New("publisher does not exist"),
		internal.ResponseCodeNoOffset:                    errors.New("no offset"),
	}
)

var schemePorts = map[string]int{"rabbitmq-stream": 5552, "rabbitmq-stream+tls": 5551}

const (
	defaultConnectionTimeout = 30 * time.Second
	debugLevel               = 1
	traceLevel               = 2
	fineLevel                = 5
)

type Clienter interface {
	Connect(ctx context.Context) error
	DeclareStream(ctx context.Context, name string) error
	IsOpen() bool
	Close(ctx context.Context) error
}

type broker struct {
	Host     string
	Port     int
	Username string
	Vhost    string
	Password string
	Scheme   string
	AdvHost  string
	AdvPort  string
}

var defaultBroker = broker{
	Host:     "localhost",
	Port:     5552,
	Username: "guest",
	Vhost:    "/",
	Password: "guest",
	Scheme:   "rabbitmq-stream",
	AdvHost:  "",
	AdvPort:  "",
}

type RawClientConfiguration struct {
	rabbitmqBrokers    []broker
	clientMaxFrameSize uint32
	clientHeartbeat    uint32
	authMechanism      []string
	tlsConfig          *tls.Config
	dial               func(network, addr string) (net.Conn, error)
}

func (r *RawClientConfiguration) TlsConfig() *tls.Config {
	return r.tlsConfig
}

func (r *RawClientConfiguration) SetTlsConfig(tlsConfig *tls.Config) {
	r.tlsConfig = tlsConfig
}

func (r *RawClientConfiguration) SetDial(dial func(network, addr string) (net.Conn, error)) {
	r.dial = dial
}

func (r *RawClientConfiguration) RabbitmqBrokers() []broker {
	return r.rabbitmqBrokers
}

func (r *RawClientConfiguration) SetClientMaxFrameSize(clientMaxFrameSize uint32) {
	r.clientMaxFrameSize = clientMaxFrameSize
}

func (r *RawClientConfiguration) SetClientHeartbeat(clientHeartbeat uint32) {
	r.clientHeartbeat = clientHeartbeat
}

func NewRawClientConfiguration(rabbitmqUrls ...string) (*RawClientConfiguration, error) {
	builder := &RawClientConfiguration{
		rabbitmqBrokers:    make([]broker, 0, 9),
		clientHeartbeat:    60,
		clientMaxFrameSize: 1_048_576,
	}

	if rabbitmqUrls == nil || len(rabbitmqUrls) == 0 {
		builder.rabbitmqBrokers = append(builder.rabbitmqBrokers, defaultBroker)
		return builder, nil
	}

	for _, uri := range rabbitmqUrls {
		broker, err := parseURI(uri)
		if err != nil {
			return nil, err
		}
		builder.rabbitmqBrokers = append(builder.rabbitmqBrokers, broker)
	}
	return builder, nil
}
