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
	errURIScheme           = errors.New("RabbitMQ Stream scheme must be either 'rabbitmq-stream://' or 'rabbitmq-stream+tls://'")
	errURIWhitespace       = errors.New("URI must not contain whitespace")
	errNilContext          = errors.New("context cannot be nil")
	errNilConfig           = errors.New("RabbitmqConfiguration cannot be nil")
	errUnknownSubscription = errors.New("unknown subscription ID")
	errNoMoreBrokersToTry  = errors.New("failed to dial RabbitMQ: no more brokers to try")
	errWriteShort          = errors.New("wrote less bytes than expected")
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

	if len(rabbitmqUrls) == 0 {
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
type Chunk = internal.ChunkResponse
type CreditError = internal.CreditResponse
type MetadataResponse = internal.MetadataResponse
type MetadataUpdate = internal.MetadataUpdateResponse
type ConsumerUpdate = internal.ConsumerUpdateQuery

type Clienter interface {
	Connect(ctx context.Context) error
	DeclareStream(ctx context.Context, stream string, configuration constants.StreamConfiguration) error
	DeleteStream(ctx context.Context, stream string) error
	DeclarePublisher(ctx context.Context, publisherId uint8, publisherReference string, stream string) error
	Send(ctx context.Context, publisherId uint8, messages []common.PublishingMessager) error
	SendSubEntryBatch(ctx context.Context, publisherId uint8, publishingId uint64, compress common.CompresserCodec, publishingMessages []common.PublishingMessager) error
	DeletePublisher(ctx context.Context, publisherId uint8) error
	Subscribe(ctx context.Context, stream string, offsetType uint16, subscriptionId uint8, credit uint16, properties constants.SubscribeProperties, offset uint64) error
	Unsubscribe(ctx context.Context, subscriptionId uint8) error
	IsOpen() bool
	Close(ctx context.Context) error
	NotifyPublish(chan *PublishConfirm) <-chan *PublishConfirm
	NotifyChunk(c chan *Chunk) <-chan *Chunk
	ExchangeCommandVersions(ctx context.Context) error
	Credit(ctx context.Context, subscriptionId uint8, credit uint16) error
	NotifyCreditError(notification chan *CreditError) <-chan *CreditError
	MetadataQuery(ctx context.Context, stream string) (*MetadataResponse, error)
	StoreOffset(ctx context.Context, reference, stream string, offset uint64) error
	QueryOffset(ctx context.Context, reference string, stream string) (uint64, error)
	StreamStats(ctx context.Context, stream string) (map[string]int64, error)
	QueryPublisherSequence(ctx context.Context, reference, stream string) (uint64, error)
	Partitions(ctx context.Context, superStream string) ([]string, error)
	RouteQuery(ctx context.Context, routingKey, superStream string) ([]string, error)
	NotifyMetadata() <-chan *MetadataUpdate
	NotifyConsumerUpdate(chan *ConsumerUpdate) <-chan *ConsumerUpdate
	ConsumerUpdateResponse(ctx context.Context, correlationId uint32, responseCode uint16, offsetType uint16, offset uint64) error
}
