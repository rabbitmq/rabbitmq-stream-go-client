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
	client               raw.Clienter
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
		client:               nil,
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

	l.client = client
	l.isSet = true

	l.clientClose = client.NotifyConnectionClosed()
	go l.shutdownHandler()

	// TODO: exchange command versions

	return nil
}

func (l *locator) maybeInitializeLocator() error {
	l.Lock()
	defer l.Unlock()

	if l.isSet && l.client.IsOpen() {
		l.log.Debug("locator is already initialized and connected")
		return nil
	}

	ctx := raw.NewContextWithLogger(context.Background(), *l.log)

	return l.connect(ctx)
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
				dialCtx, cancel := context.WithTimeout(raw.NewContextWithLogger(context.Background(), *log), DefaultTimeout)
				c, e := raw.DialConfig(dialCtx, &l.rawClientConf)
				cancel()

				if e != nil && i == 99 {
					log.Debug("maximum number of attempts trying to reconnect locator, giving up", slog.Any("error", e), slog.Int("attempt", i))
					l.isSet = false
					l.client = nil
					l.Unlock()
					return
				}

				if e != nil {
					log.Debug("error recovering locator connection, trying again in 5 seconds", slog.Any("error", err))
					<-time.After(time.Second * 5)
					continue
				}

				l.client = c
				l.clientClose = c.NotifyConnectionClosed()

				log.Debug("locator reconnected")

				// TODO: exchange command versions
				break
			}
			l.Unlock()
		}
	}
}

// locatorOperationFn type represents a "generic" operation on a locator. The
// implementing function must always return an error, or nil, as last element of
// the result slice. Most implementations are likely to return a slice with a
// single nil-error element. Implementations that return a result and an error, like
// e.g. query metadata, must return a result-element first, and an error (or nil) as
// last element.
type locatorOperationFn func(*locator, ...any) (result []any)

func (l *locator) operationCreateStream(args ...any) []any {
	ctx := args[0].(context.Context)
	name := args[1].(string)
	configuration := args[2].(raw.StreamConfiguration)
	resultErr := l.client.DeclareStream(ctx, name, configuration)
	return []any{resultErr}
}

func (l *locator) locatorOperation(op locatorOperationFn, args ...any) (result []any) {
	l.log.Debug("starting locator operation")

	var lastErr error
	for attempt := 0; attempt < maxAttempt; {
		result = op(l, args...)

		// last element of result is error type
		if result[len(result)-1] == nil {
			lastErr = nil
			l.log.Debug("locator operation succeed")
			break
		}

		lastErr = result[len(result)-1].(error)
		if isNonRetryableError(lastErr) || errors.Is(lastErr, context.DeadlineExceeded) {
			l.log.Debug("locator operation failed with non-retryable error", slog.Any("error", lastErr))
			return result
		}

		l.log.Debug("error in locator operation", slog.Any("error", lastErr), slog.Int("attempt", attempt))
		attempt++
	}
	return result
}
