package stream

import (
	"context"
	"github.com/rabbitmq/rabbitmq-stream-go-client/v2/pkg/raw"
	"golang.org/x/exp/slog"
	"golang.org/x/mod/semver"
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

	if l.isServer311orMore() {
		return l.client.ExchangeCommandVersions(ctx)
	}

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

const rabbitmqVersion311 = "v3.11"

func (l *locator) isServer311orMore() bool {
	v, ok := l.rawClientConf.RabbitmqBrokers().ServerProperties["version"]
	if !ok {
		// version not found in server properties
		// returning false as we can't determine
		return false
	}
	// "v" + v because semver module expects all string versions to start with v
	return semver.Compare("v"+v, rabbitmqVersion311) >= 0
}

// locatorOperationFn type represents a "generic" operation on a locator. The
// implementing function must always return an error, or nil, as last element of
// the result slice. Most implementations are likely to return a slice with a
// single nil-error element. Implementations that return a result and an error, like
// e.g. query metadata, must return a result-element first, and an error (or nil) as
// last element.
type locatorOperationFn func(*locator, ...any) (result []any)

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
		if isNonRetryableError(lastErr) {
			l.log.Debug("locator operation failed with non-retryable error", slog.Any("error", lastErr))
			return result
		}

		l.log.Debug("error in locator operation", slog.Any("error", lastErr), slog.Int("attempt", attempt))
		attempt++
	}
	return result
}

func (l *locator) operationCreateStream(args ...any) []any {
	ctx := args[0].(context.Context)
	name := args[1].(string)
	configuration := args[2].(raw.StreamConfiguration)
	return []any{l.client.DeclareStream(ctx, name, configuration)}
}

func (l *locator) operationDeleteStream(args ...any) []any {
	ctx := args[0].(context.Context)
	name := args[1].(string)
	return []any{l.client.DeleteStream(ctx, name)}
}

func (l *locator) operationQueryStreamStats(args ...any) []any {
	if !l.isServer311orMore() {
		return []any{nil, ErrUnsupportedOperation}
	}
	ctx := args[0].(context.Context)
	name := args[1].(string)
	stats, err := l.client.StreamStats(ctx, name)
	return []any{stats, err}
}

func (l *locator) operationQueryOffset(args ...any) []any {
	ctx := args[0].(context.Context)
	reference := args[1].(string)
	stream := args[2].(string)
	offset, err := l.client.QueryOffset(ctx, reference, stream)
	return []any{offset, err}
}
func (l *locator) operationPartitions(args ...any) []any {
	ctx := args[0].(context.Context)
	superstream := args[1].(string)
	partitions, err := l.client.Partitions(ctx, superstream)
	return []any{partitions, err}
}

func (l *locator) operationQuerySequence(args ...any) []any {
	ctx := args[0].(context.Context)
	reference := args[1].(string)
	stream := args[2].(string)
	pubId, err := l.client.QueryPublisherSequence(ctx, reference, stream)
	return []any{pubId, err}
}
