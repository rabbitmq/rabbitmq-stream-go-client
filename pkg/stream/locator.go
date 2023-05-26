package stream

import (
	"context"
	"github.com/gsantomaggio/rabbitmq-stream-go-client/pkg/raw"
	"net"
	"sync"
)

type locator struct {
	sync.Mutex
	rawClientConf   raw.ClientConfiguration
	Client          raw.Clienter
	isSet           bool
	addressResolver net.Addr // TODO: placeholder for address resolver

}

func newLocator(c raw.ClientConfiguration) *locator {
	return &locator{
		rawClientConf:   c,
		Client:          nil,
		isSet:           false,
		addressResolver: nil,
	}
}

func (l *locator) connect(ctx context.Context) error {
	l.Lock()
	defer l.Unlock()
	// if ctx has a lower timeout, it will be used instead of DefaultTimeout
	// https://pkg.go.dev/context#WithDeadline
	dialCtx, cancel := context.WithTimeout(ctx, DefaultTimeout)
	defer cancel()

	client, err := raw.DialConfig(dialCtx, &l.rawClientConf)
	if err != nil {
		return err
	}

	l.Client = client
	l.isSet = true

	return nil
}
