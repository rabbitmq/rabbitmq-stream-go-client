package stream

import (
	"github.com/gsantomaggio/rabbitmq-stream-go-client/pkg/raw"
	"net"
)

type locator struct {
	Client          raw.Clienter
	addressResolver net.Addr // TODO: placeholder for address resolver
}
