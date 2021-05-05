package streaming

import (
	"time"
)

const (
	commandDeclarePublisher       = 1
	commandPublish                = 2
	commandPublishConfirm         = 3
	commandPublishError           = 4
	commandQueryPublisherSequence = 5
	commandDeletePublisher        = 6
	commandSubscribe              = 7
	commandDeliver                = 8
	commandCredit                 = 9
	commandCommitOffset           = 10
	commandQueryOffset            = 11
	commandUnsubscribe            = 12
	commandCreateStream           = 13
	commandDeleteStream           = 14
	commandMetadata               = 15
	commandMetadataUpdate         = 16
	commandPeerProperties         = 17
	commandSaslHandshake          = 18
	commandSaslAuthenticate       = 19
	commandTune                   = 20
	commandOpen                   = 21
	commandClose                  = 22
	commandHeartbeat              = 23

	version1    = 1
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

	/// responses out of protocol
	closeChannel = uint16(60)
	///
	defaultSocketCallTimeout = 4 * time.Second

	///
	LocalhostUriConnection = "rabbitmq-streaming://guest:guest@localhost:5551/%2f"

	///
	defaultReadSocketBuffer = 4096 * 2

	//
	clientVersion = "0.4-alpha"
)

func lookErrorCode(errorCode uint16) string {
	switch errorCode {
	case responseCodeOk:
		return "OK"
	case responseCodeAuthenticationFailure:
		return "authentication failure"
	case responseCodeStreamDoesNotExist:
		return "stream does not exist"
	case responseCodeStreamAlreadyExists:
		return "stream already exists"
	case responseCodeVirtualHostAccessFailure:
		return "virtualHost access failure"
	case responseCodeSubscriptionIdDoesNotExist:
		return "subscription id does not exist"
	case responseCodePublisherDoesNotExist:
		return "publisher does not exist"
	case responseCodePreconditionFailed:
		return "precondition failed"
	default:
		{
			WARN("Error not handled %d", errorCode)
			return "Error not handled"
		}
	}

}
