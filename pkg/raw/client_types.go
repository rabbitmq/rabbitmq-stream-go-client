package raw

import (
	"context"
	"crypto/tls"
	"errors"
	"github.com/rabbitmq/rabbitmq-stream-go-client/v2/internal"
	"github.com/rabbitmq/rabbitmq-stream-go-client/v2/pkg/common"
	"net"
	"time"
)

var (
	errURIScheme        = errors.New("RabbitMQ Stream scheme must be either 'rabbitmq-stream://' or 'rabbitmq-stream+tls://'")
	errURIWhitespace    = errors.New("URI must not contain whitespace")
	errNilContext       = errors.New("context cannot be nil")
	errNilConfig        = errors.New("RabbitmqConfiguration cannot be nil")
	errWriteShort       = errors.New("wrote less bytes than expected")
	ErrConnectionClosed = errors.New("connection closed by peer. EOF error")
)

var schemePorts = map[string]int{"rabbitmq-stream": 5552, "rabbitmq-stream+tls": 5551}

const (
	defaultConnectionTimeout = 30 * time.Second
)

type RabbitmqAddress struct {
	Host             string
	Port             int
	Username         string
	Vhost            string
	Password         string
	Scheme           string
	ServerProperties map[string]string
}

var defaultBroker = RabbitmqAddress{
	Host:     "localhost",
	Port:     5552,
	Username: "guest",
	Vhost:    "/",
	Password: "guest",
	Scheme:   "rabbitmq-stream",
}

type ClientConfiguration struct {
	RabbitmqAddr       RabbitmqAddress
	ClientMaxFrameSize uint32
	ClientHeartbeat    uint32
	AuthMechanism      []string
	Tls                *tls.Config
	ConnectionName     string
	Dial               func(network, addr string) (net.Conn, error)
}

func (r *ClientConfiguration) DeepCopy() *ClientConfiguration {
	deepCopy := new(ClientConfiguration)
	deepCopy.RabbitmqAddr = r.RabbitmqAddr
	deepCopy.ClientMaxFrameSize = r.ClientMaxFrameSize
	deepCopy.ClientHeartbeat = r.ClientHeartbeat
	deepCopy.AuthMechanism = make([]string, len(r.AuthMechanism))
	copy(deepCopy.AuthMechanism, r.AuthMechanism)
	deepCopy.Tls = r.Tls.Clone()
	deepCopy.Dial = r.Dial
	return deepCopy
}

func NewClientConfiguration(rabbitmqUrl string) (*ClientConfiguration, error) {
	builder := &ClientConfiguration{
		RabbitmqAddr:       RabbitmqAddress{},
		ClientHeartbeat:    60,
		ClientMaxFrameSize: 1_048_576,
	}

	if len(rabbitmqUrl) == 0 {
		builder.RabbitmqAddr = defaultBroker
		return builder, nil
	}

	broker, err := parseURI(rabbitmqUrl)
	if err != nil {
		return nil, err
	}

	builder.RabbitmqAddr = broker
	return builder, nil
}

type PublishConfirm = internal.PublishConfirmResponse
type PublishError = internal.PublishErrorResponse
type PublishingError = internal.PublishingError
type Chunk = internal.ChunkResponse
type CreditError = internal.CreditResponse
type MetadataResponse = internal.MetadataResponse
type MetadataUpdate = internal.MetadataUpdateResponse
type ConsumerUpdate = internal.ConsumerUpdateQuery
type Heartbeat = internal.Heartbeat

type Clienter interface {
	Connect(ctx context.Context) error
	DeclareStream(ctx context.Context, stream string, configuration StreamConfiguration) error
	DeleteStream(ctx context.Context, stream string) error
	DeclarePublisher(ctx context.Context, publisherId uint8, publisherReference string, stream string) error
	Send(ctx context.Context, publisherId uint8, messages []common.PublishingMessager) error
	SendSubEntryBatch(ctx context.Context, publisherId uint8, publishingId uint64, compress common.CompresserCodec, messages []common.Message) error
	DeletePublisher(ctx context.Context, publisherId uint8) error
	Subscribe(ctx context.Context, stream string, offsetType uint16, subscriptionId uint8, credit uint16, properties SubscribeProperties, offset uint64) error
	Unsubscribe(ctx context.Context, subscriptionId uint8) error
	IsOpen() bool
	Close(ctx context.Context) error
	NotifyPublish(chan *PublishConfirm) <-chan *PublishConfirm
	NotifyChunk(c chan *Chunk) <-chan *Chunk
	NotifyConnectionClosed() <-chan error
	ExchangeCommandVersions(ctx context.Context) error
	Credit(ctx context.Context, subscriptionId uint8, credit uint16) error
	NotifyCreditError(notification chan *CreditError) <-chan *CreditError
	MetadataQuery(ctx context.Context, streams []string) (*MetadataResponse, error)
	StoreOffset(ctx context.Context, reference, stream string, offset uint64) error
	QueryOffset(ctx context.Context, reference string, stream string) (uint64, error)
	StreamStats(ctx context.Context, stream string) (map[string]int64, error)
	QueryPublisherSequence(ctx context.Context, reference, stream string) (uint64, error)
	Partitions(ctx context.Context, superStream string) ([]string, error)
	RouteQuery(ctx context.Context, routingKey, superStream string) ([]string, error)
	NotifyMetadata() <-chan *MetadataUpdate
	NotifyConsumerUpdate() <-chan *ConsumerUpdate
	ConsumerUpdateResponse(ctx context.Context, correlationId uint32, responseCode uint16, offsetType uint16, offset uint64) error
	NotifyPublishError() <-chan *PublishError
	SendHeartbeat() error
	NotifyHeartbeat() <-chan *Heartbeat
}

var (
	ErrStreamDoesNotExist          = errors.New("stream does not exist")
	ErrSubscriptionIdAlreadyExists = errors.New("subscription ID already exists")
	ErrSubscriptionIdDoesNotExist  = errors.New("subscription ID does not exist")
	ErrStreamAlreadyExists         = errors.New("stream already exists")
	ErrStreamNotAvailable          = errors.New("stream not available")
	ErrSASLMechanismNotSupported   = errors.New("SASL mechanism not supported")
	ErrAuthFailure                 = errors.New("authentication failure")
	ErrSASLError                   = errors.New("SASL error")
	ErrSASLChallenge               = errors.New("SASL challenge")
	ErrSASLAuthFailureLoopback     = errors.New("SASL authentication failure loopback")
	ErrVirtualHostAccessFailure    = errors.New("virtual host access failure")
	ErrUnknownFrame                = errors.New("unknown frame")
	ErrFrameTooLarge               = errors.New("frame too large")
	ErrInternalError               = errors.New("internal error")
	ErrAccessRefused               = errors.New("access refused")
	ErrPreconditionFailed          = errors.New("precondition failed")
	ErrPublisherDoesNotExist       = errors.New("publisher does not exist")
	ErrNoOffset                    = errors.New("no offset found")
)

var ResponseCodeToError = map[uint16]error{
	ResponseCodeOK:                          nil, // this is a special case where there is not error
	ResponseCodeStreamDoesNotExist:          ErrStreamDoesNotExist,
	ResponseCodeSubscriptionIdAlreadyExists: ErrSubscriptionIdAlreadyExists,
	ResponseCodeSubscriptionIdDoesNotExist:  ErrSubscriptionIdDoesNotExist,
	ResponseCodeStreamAlreadyExists:         ErrStreamAlreadyExists,
	ResponseCodeStreamNotAvailable:          ErrStreamNotAvailable,
	ResponseCodeSASLMechanismNotSupported:   ErrSASLMechanismNotSupported,
	ResponseCodeAuthFailure:                 ErrAuthFailure,
	ResponseCodeSASLError:                   ErrSASLError,
	ResponseCodeSASLChallenge:               ErrSASLChallenge,
	ResponseCodeSASLAuthFailureLoopback:     ErrSASLAuthFailureLoopback,
	ResponseCodeVirtualHostAccessFailure:    ErrVirtualHostAccessFailure,
	ResponseCodeUnknownFrame:                ErrUnknownFrame,
	ResponseCodeFrameTooLarge:               ErrFrameTooLarge,
	ResponseCodeInternalError:               ErrInternalError,
	ResponseCodeAccessRefused:               ErrAccessRefused,
	ResponseCodePreconditionFailed:          ErrPreconditionFailed,
	ResponseCodePublisherDoesNotExist:       ErrPublisherDoesNotExist,
	ResponseCodeNoOffset:                    ErrNoOffset,
}

// Stream protocol response codes
const (
	ResponseCodeOK                          uint16 = 0x01
	ResponseCodeStreamDoesNotExist          uint16 = 0x02
	ResponseCodeSubscriptionIdAlreadyExists uint16 = 0x03
	ResponseCodeSubscriptionIdDoesNotExist  uint16 = 0x04
	ResponseCodeStreamAlreadyExists         uint16 = 0x05
	ResponseCodeStreamNotAvailable          uint16 = 0x06
	ResponseCodeSASLMechanismNotSupported   uint16 = 0x07
	ResponseCodeAuthFailure                 uint16 = 0x08
	ResponseCodeSASLError                   uint16 = 0x09
	ResponseCodeSASLChallenge               uint16 = 0x0a
	ResponseCodeSASLAuthFailureLoopback     uint16 = 0x0b
	ResponseCodeVirtualHostAccessFailure    uint16 = 0x0c
	ResponseCodeUnknownFrame                uint16 = 0x0d
	ResponseCodeFrameTooLarge               uint16 = 0x0e
	ResponseCodeInternalError               uint16 = 0x0f
	ResponseCodeAccessRefused               uint16 = 0x10
	ResponseCodePreconditionFailed          uint16 = 0x11
	ResponseCodePublisherDoesNotExist       uint16 = 0x12
	ResponseCodeNoOffset                    uint16 = 0x13
)

// Connections states
const (
	ConnectionClosed  = 0x01
	ConnectionOpen    = 0x02
	ConnectionClosing = 0x03
)

// TODO: go docs
type StreamConfiguration = map[string]string

// TODO: go docs
type SubscribeProperties = map[string]string
