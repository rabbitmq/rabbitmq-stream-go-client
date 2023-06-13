package stream

import (
	"context"
	"errors"
	"github.com/gsantomaggio/rabbitmq-stream-go-client/pkg/raw"
	"golang.org/x/exp/slog"
	"net"
	"sync"
	"time"
)

const (
	maxAttempt = 3
)

type locator struct {
	sync.Mutex
	log                  *slog.Logger
	shutdownNotification chan struct{}
	rawClientConf        raw.ClientConfiguration
	Client               raw.Clienter
	isSet                bool
	clientClose          <-chan error
	backOffPolicy        func(int) time.Duration
	addressResolver      net.Addr // TODO: placeholder for address resolver

}

func newLocator(c raw.ClientConfiguration, logger *slog.Logger) *locator {
	return &locator{
		log: logger.
			WithGroup("locator").
			With(
				slog.String("host", c.RabbitmqBrokers().Host),
				slog.Int("port", c.RabbitmqBrokers().Port),
			),
		rawClientConf: c,
		backOffPolicy: func(attempt int) time.Duration {
			return time.Second * time.Duration(attempt<<1)
		},
		Client:               nil,
		isSet:                false,
		addressResolver:      nil,
		shutdownNotification: make(chan struct{}),
	}
}

func (l *locator) connect(ctx context.Context) error {
	l.Lock()
	defer l.Unlock()
	client, err := raw.DialConfig(ctx, &l.rawClientConf)
	if err != nil {
		return err
	}

	l.Client = client
	l.isSet = true

	l.clientClose = client.NotifyConnectionClosed()
	go l.shutdownHandler()

	// TODO: exchange command versions

	return nil
}

func (l *locator) maybeInitializeLocator() error {
	l.Lock()
	defer l.Unlock()

	if l.isSet {
		return nil
	}

	return l.connect(context.Background())
}

// this function is meant to run in a routine. Do not call it directly.
func (l *locator) shutdownHandler() {
	log := l.log.WithGroup("shutdownHandler")
	for {
		// TODO: we must close the shutdown notification channel before closing the client.
		//   Or otherwise the clientClose case will proceed (incorrectly).
		select {
		case <-l.shutdownNotification:
			log.Debug("locator shutdown")
			return
		case err := <-l.clientClose:
			// TODO: maybe add a 'ok' safeguard here?
			log.Debug("unexpected locator disconnection, trying to reconnect", slog.Any("error", err))
			l.Lock()
			for i := 0; i < 100; i++ {
				dialCtx, cancel := context.WithTimeout(context.Background(), DefaultTimeout)
				c, e := raw.DialConfig(dialCtx, &l.rawClientConf)
				cancel()

				if e != nil && i == 99 {
					log.Debug("maximum number of attempts trying to reconnect locator, giving up", slog.Any("error", e), slog.Int("attempt", i))
					l.isSet = false
					l.Client = nil
					l.Unlock()
					return
				}

				if e != nil {
					log.Debug("error recovering locator connection, trying again in 5 seconds", slog.Any("error", err))
					<-time.After(time.Second * 5)
					continue
				}

				l.Client = c
				l.clientClose = c.NotifyConnectionClosed()

				log.Debug("locator reconnected")

				// TODO: exchange command versions
				break
			}
			l.Unlock()
		}
	}
}

func (l *locator) createStream(ctx context.Context, name string, configuration raw.StreamConfiguration) error {
	l.log.Debug("starting locator operation 'create stream'", slog.String("streamName", name))

	if err := l.maybeInitializeLocator(); err != nil {
		return err
	}

	var lastErr error
	for attempt := 0; attempt < maxAttempt; {
		lastErr = l.Client.DeclareStream(ctx, name, configuration)
		if lastErr == nil {
			l.log.Debug("locator operation 'create stream' succeed", slog.String("streamName", name))
			break
		}

		if errors.Is(lastErr, raw.ErrStreamAlreadyExists) {
			l.log.Debug("locator operation 'create stream' failed with non-retryable error", slog.String("streamName", name))
			return lastErr
		}

		l.log.Debug("error in locator operation 'create stream'", slog.Any("error", lastErr), slog.Int("attempt", attempt))

		attempt++

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(l.backOffPolicy(attempt)):
		}
	}

	return lastErr
}
