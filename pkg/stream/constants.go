package stream

import (
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/logs"
)

// is needed to indicate the general status
// for example the producer status

const (
	open   = iota
	closed = iota
)

const initBufferPublishSize = 2 + 2 + 1 + 4

const (
	ClientVersion = "1.4.9"

	commandDeclarePublisher       = 1
	commandPublish                = 2
	commandPublishConfirm         = 3
	commandPublishError           = 4
	commandQueryPublisherSequence = 5
	CommandDeletePublisher        = 6
	commandSubscribe              = 7
	commandDeliver                = 8
	commandCredit                 = 9
	commandStoreOffset            = 10
	CommandQueryOffset            = 11
	CommandUnsubscribe            = 12
	commandCreateStream           = 13
	commandDeleteStream           = 14
	commandMetadata               = 15
	CommandMetadataUpdate         = 16
	commandPeerProperties         = 17
	commandSaslHandshake          = 18
	commandSaslAuthenticate       = 19
	commandTune                   = 20
	commandOpen                   = 21
	CommandClose                  = 22
	commandHeartbeat              = 23
	commandQueryRoute             = 24
	commandQueryPartition         = 25
	consumerUpdateQueryResponse   = 26
	commandExchangeVersion        = 27
	commandStreamStatus           = 28
	commandCreateSuperStream      = 29
	commandDeleteSuperStream      = 30
	commandConsumerUpdate         = 0x801a

	/// used only for tests
	commandUnitTest = 99

	version1    = 1
	version2    = 2
	unicodeNull = "\u0000"

	responseCodeOk                            = uint16(1)
	responseCodeStreamDoesNotExist            = uint16(2)
	responseCodeSubscriptionIdAlreadyExists   = uint16(3)
	responseCodeSubscriptionIdDoesNotExist    = uint16(4)
	responseCodeStreamAlreadyExists           = uint16(5)
	responseCodeStreamNotAvailable            = uint16(6)
	responseCodeSaslMechanismNotSupported     = uint16(7)
	responseCodeAuthenticationFailure         = uint16(8)
	responseCodeSaslError                     = uint16(9)
	responseCodeSaslChallenge                 = uint16(10)
	responseCodeAuthenticationFailureLoopback = uint16(11)
	responseCodeVirtualHostAccessFailure      = uint16(12)
	responseCodeUnknownFrame                  = uint16(13)
	responseCodeFrameTooLarge                 = uint16(14)
	responseCodeInternalError                 = uint16(15)
	responseCodeAccessRefused                 = uint16(16)
	responseCodePreconditionFailed            = uint16(17)
	responseCodePublisherDoesNotExist         = uint16(18)
	responseCodeNoOffset                      = uint16(19)

	/// responses out of protocol
	closeChannel         = uint16(60)
	connectionCloseError = uint16(61)
	timeoutError         = uint16(62)

	///
	defaultSocketCallTimeout = 10 * time.Second

	//
	LocalhostUriConnection = "rabbitmq-stream://guest:guest@localhost:5552/%2f"

	///
	defaultWriteSocketBuffer  = 8092
	defaultReadSocketBuffer   = 65536
	defaultQueuePublisherSize = 10000
	minQueuePublisherSize     = 100
	maxQueuePublisherSize     = 1_000_000

	minBatchSize = 1
	maxBatchSize = 10_000

	minSubEntrySize = 1
	maxSubEntrySize = 65535

	minBatchPublishingDelay = 1
	maxBatchPublishingDelay = 500

	defaultBatchSize            = 100
	defaultBatchPublishingDelay = 100
	defaultConfirmationTimeOut  = 10 * time.Second
	//

	SocketClosed             = "socket client closed"
	MetaDataUpdate           = "metadata Data update"
	LeaderLocatorBalanced    = "balanced"
	LeaderLocatorClientLocal = "client-local"

	StreamTcpPort = "5552"

	subscriptionPropertyFilterPrefix    = "filter."
	subscriptionPropertyMatchUnfiltered = "match-unfiltered"
)

var AlreadyClosed = errors.New("Already Closed")

var PreconditionFailed = errors.New("Precondition Failed")
var AuthenticationFailure = errors.New("Authentication Failure")
var StreamDoesNotExist = errors.New("Stream Does Not Exist")
var StreamAlreadyExists = errors.New("Stream Already Exists")
var VirtualHostAccessFailure = errors.New("Virtual Host Access Failure")
var SubscriptionIdDoesNotExist = errors.New("Subscription Id Does Not Exist")
var PublisherDoesNotExist = errors.New("Publisher Does Not Exist")
var OffsetNotFoundError = errors.New("Offset not found")
var FrameTooLarge = errors.New("Frame Too Large, the buffer is too big")
var CodeAccessRefused = errors.New("Resources Access Refused")
var ConnectionClosed = errors.New("Can't Send the message, connection closed")
var StreamNotAvailable = errors.New("Stream Not Available")
var UnknownFrame = errors.New("Unknown Frame")
var InternalError = errors.New("Internal Error")
var AuthenticationFailureLoopbackError = errors.New("Authentication Failure Loopback Error")
var ConfirmationTimoutError = errors.New("Confirmation Timeout Error")
var FilterNotSupported = errors.New("Filtering is not supported by the broker " +
	"(requires RabbitMQ 3.13+ and stream_filtering feature flag activated)")
var SingleActiveConsumerNotSupported = errors.New("Single Active Consumer is not supported by the broker " +
	"(requires RabbitMQ 3.11+ and stream_single_active_consumer feature flag activated)")

var ErrProducerNotFound = errors.New("Producer not found in the SuperStream Producer")
var ErrMessageRouteNotFound = errors.New("Message Route not found for the message key")
var ErrSuperStreamProducerOptionsNotDefined = errors.New("SuperStreamProducerOptions not defined. The SuperStreamProducerOptions is mandatory with the RoutingStrategy")
var ErrSuperStreamConsumerOptionsNotDefined = errors.New("SuperStreamConsumerOptions not defined.")

var ErrEnvironmentNotDefined = errors.New("Environment not defined")

var LeaderNotReady = errors.New("Leader not Ready yet")

func lookErrorCode(errorCode uint16) error {
	switch errorCode {
	case responseCodeOk:
		return nil
	case responseCodeAuthenticationFailure:
		return AuthenticationFailure
	case responseCodeStreamDoesNotExist:
		return StreamDoesNotExist
	case responseCodeStreamAlreadyExists:
		return StreamAlreadyExists
	case responseCodeVirtualHostAccessFailure:
		return VirtualHostAccessFailure
	case responseCodeSubscriptionIdDoesNotExist:
		return SubscriptionIdDoesNotExist
	case responseCodePublisherDoesNotExist:
		return PublisherDoesNotExist
	case responseCodeNoOffset:
		return OffsetNotFoundError
	case responseCodePreconditionFailed:
		return PreconditionFailed
	case responseCodeFrameTooLarge:
		return FrameTooLarge
	case responseCodeAccessRefused:
		return CodeAccessRefused
	case responseCodeStreamNotAvailable:
		return StreamNotAvailable
	case responseCodeUnknownFrame:
		return UnknownFrame
	case responseCodeInternalError:
		return InternalError
	case responseCodeAuthenticationFailureLoopback:
		return AuthenticationFailureLoopbackError
	default:
		{
			logs.LogWarn("Error not handled %d", errorCode)
			return errors.New("Generic Error")
		}
	}
}

func lookUpCommand(command uint16) string {
	var constLookup = map[uint16]string{
		commandPeerProperties:   `commandPeerProperties`,
		commandSaslHandshake:    `commandSaslHandshake`,
		commandSaslAuthenticate: `commandSaslAuthenticate`,
		commandTune:             `commandTune`,
		commandOpen:             `commandOpen`,
		commandHeartbeat:        `commandHeartbeat`,
		CommandMetadataUpdate:   `CommandMetadataUpdate`,
		commandMetadata:         `CommandMetadata`,
		commandDeleteStream:     `CommandDeleteStream`,
		commandCreateStream:     `CommandCreateStream`,
		CommandUnsubscribe:      `CommandUnsubscribe`,
		CommandQueryOffset:      `CommandQueryOffset`,
		commandCredit:           `CommandCredit`,
		commandDeliver:          `CommandDeliver`,
		commandSubscribe:        `CommandSubscribe`,
		CommandDeletePublisher:  `CommandDeletePublisher`,
		commandPublishError:     `CommandPublishError`,
		commandPublishConfirm:   `CommandPublishConfirm`,
		commandDeclarePublisher: `CommandDeclarePublisher`,
		commandUnitTest:         `UnitTest`,
		CommandClose:            `CommandClose`,
	}
	if constLookup[command] == "" {
		return fmt.Sprintf("Command not handled %d", command)
	}

	return constLookup[command]
}

const (
	SaslConfigurationPlain    = "PLAIN"
	SaslConfigurationExternal = "EXTERNAL"
)
