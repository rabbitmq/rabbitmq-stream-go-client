package raw

import (
	"context"
	"crypto/tls"
	"errors"
	"github.com/gsantomaggio/rabbitmq-stream-go-client/internal"
	"github.com/gsantomaggio/rabbitmq-stream-go-client/pkg/common"
	"github.com/gsantomaggio/rabbitmq-stream-go-client/pkg/constants"
	"net"
	"time"
)

var (
	errURIScheme     = errors.New("RabbitMQ Stream scheme must be either 'rabbitmq-stream://' or 'rabbitmq-stream+tls://'")
	errURIWhitespace = errors.New("URI must not contain whitespace")
	errNilContext    = errors.New("context cannot be nil")
	errNilConfig     = errors.New("RabbitmqConfiguration cannot be nil")
)

var schemePorts = map[string]int{"rabbitmq-stream": 5552, "rabbitmq-stream+tls": 5551}

const (
	defaultConnectionTimeout = 30 * time.Second
	debugLevel               = 1
	traceLevel               = 2
	fineLevel                = 5
)

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

type ClientConfiguration struct {
	rabbitmqBrokers    []broker
	clientMaxFrameSize uint32
	clientHeartbeat    uint32
	authMechanism      []string
	tlsConfig          *tls.Config
	dial               func(network, addr string) (net.Conn, error)
}

func (r *ClientConfiguration) TlsConfig() *tls.Config {
	return r.tlsConfig
}

func (r *ClientConfiguration) SetTlsConfig(tlsConfig *tls.Config) {
	r.tlsConfig = tlsConfig
}

func (r *ClientConfiguration) SetDial(dial func(network, addr string) (net.Conn, error)) {
	r.dial = dial
}

func (r *ClientConfiguration) RabbitmqBrokers() []broker {
	return r.rabbitmqBrokers
}

func (r *ClientConfiguration) SetClientMaxFrameSize(clientMaxFrameSize uint32) {
	r.clientMaxFrameSize = clientMaxFrameSize
}

func (r *ClientConfiguration) SetClientHeartbeat(clientHeartbeat uint32) {
	r.clientHeartbeat = clientHeartbeat
}

func NewClientConfiguration(rabbitmqUrls ...string) (*ClientConfiguration, error) {
	builder := &ClientConfiguration{
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

type PublishConfirm = internal.PublishConfirmResponse

type Clienter interface {
	Connect(ctx context.Context) error
	DeclareStream(ctx context.Context, stream string, configuration constants.StreamConfiguration) error
	DeleteStream(ctx context.Context, stream string) error
	DeclarePublisher(ctx context.Context, publisherId uint8, publisherReference string, stream string) error
	Send(ctx context.Context, publisherId uint8, messages []common.PublishingMessager) error
	DeletePublisher(ctx context.Context, publisherId uint8) error
	DeclareConsumer(ctx context.Context, subscriptionId uint8, stream string, offsetType uint16,
		offset uint64, credit uint16,
		properties constants.SubscribeProperties) error
	IsOpen() bool
	Close(ctx context.Context) error
	NotifyPublish(chan *PublishConfirm) <-chan *PublishConfirm
}
