package streaming

import (
	"time"
)

const (
	CommandDeclarePublisher       = 1
	CommandPublish                = 2
	CommandPublishConfirm         = 3
	CommandPublishError           = 4
	CommandQueryPublisherSequence = 5
	CommandDeletePublisher        = 6
	CommandSubscribe              = 7
	CommandDeliver                = 8
	CommandCredit                 = 9
	CommandCommitOffset           = 10
	CommandQueryOffset            = 11
	CommandUnsubscribe            = 12
	CommandCreateStream           = 13
	CommandDeleteStream           = 14
	CommandMetadata               = 15
	CommandMetadataUpdate         = 16
	CommandPeerProperties         = 17
	CommandSaslHandshake          = 18
	CommandSaslAuthenticate       = 19
	CommandTune                   = 20
	CommandOpen                   = 21
	CommandClose                  = 22
	CommandHeartbeat              = 23

	Version1    = 1
	UnicodeNull = "\u0000"

	ResponseCodeOk                            = uint16(1)
	ResponseCodeStreamDoesNotExist            = uint16(2)
	ResponseCodeSubscriptionIdAlreadyExists   = uint16(3)
	ResponseCodeSubscriptionIdDoesNotExist    = uint16(4)
	ResponseCodeStreamAlreadyExists           = uint16(5)
	ResponseCodeStreamNotAvailable            = uint16(6)
	ResponseCodeSaslMechanismNotSupported     = uint16(7)
	ResponseCodeAuthenticationFailure         = uint16(8)
	ResponseCodeSaslError                     = uint16(9)
	ResponseCodeSaslChallenge                 = uint16(10)
	ResponseCodeAuthenticationFailureLoopback = uint16(11)
	ResponseCodeVirtualHostAccessFailure      = uint16(12)
	ResponseCodeUnknownFrame                  = uint16(13)
	ResponseCodeFrameTooLarge                 = uint16(14)
	ResponseCodeInternalError                 = uint16(15)
	ResponseCodeAccessRefused                 = uint16(16)
	ResponseCodePreconditionFailed            = uint16(17)
	ResponseCodePublisherDoesNotExist         = uint16(18)

	/// responses out of protocol
	CloseChannel = uint16(60)
	///
	DefaultSocketCallTimeout = 3 * time.Second

	///
	LocalhostUriConnection = "rabbitmq-streaming://guest:guest@localhost:5551/%2f"


	///
	DefaultReadSocketBuffer = 4096 * 2

//
	Version = "0.4-alpha"
)

func LookErrorCode(errorCode uint16) string {
	switch errorCode {
	case ResponseCodeOk:
		return "OK"
	case ResponseCodeAuthenticationFailure:
		return "Authentication Failure"
	case ResponseCodeStreamDoesNotExist:
		return "Stream does not exist"
	case ResponseCodeStreamAlreadyExists:
		return "Stream already exists"
	case ResponseCodeVirtualHostAccessFailure:
		return "VirtualHost access failure"
	case ResponseCodeSubscriptionIdDoesNotExist:
		return "Code subscription id does not exist"
	case ResponseCodePublisherDoesNotExist:
		return "Code publisher does not exist"
	case ResponseCodePreconditionFailed:
		return "Code Precondition Failed"
	default:
		{
			WARN("Error not handled %d", errorCode)
			return "Error not handled"
		}
	}

}
