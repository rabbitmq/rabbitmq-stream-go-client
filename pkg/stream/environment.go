package stream

import (
	"context"
	"errors"
	"fmt"
	"github.com/rabbitmq/rabbitmq-stream-go-client/v2/pkg/raw"
	"log/slog"
	"math/rand"
	"strconv"
	"time"
)

const (
	// DefaultTimeout in all network calls.
	DefaultTimeout = time.Second * 30
)

type Environment struct {
	configuration           EnvironmentConfiguration
	locators                []*locator
	retryPolicy             backoffDurationFunc
	locatorSelectSequential bool
	producerManagers        []*producerManager
	addressResolver         AddressResolver
}

func NewEnvironment(ctx context.Context, configuration EnvironmentConfiguration) (*Environment, error) {
	e := &Environment{
		configuration: configuration,
		locators:      make([]*locator, 0, len(configuration.Uris)),
		retryPolicy:   defaultBackOffPolicy,
	}

	if !configuration.LazyInitialization {
		return e, e.start(ctx)
	}

	return e, nil
}

func (e *Environment) start(ctx context.Context) error {
	logger := raw.LoggerFromCtxOrDiscard(ctx)

	var lastConnectError error
	for i, uri := range e.configuration.Uris {
		c, err := raw.NewClientConfiguration(uri)
		if err != nil {
			return err
		}

		c.ConnectionName = fmt.Sprintf("%s-locator-%d", e.configuration.Id, i)

		l := newLocator(*c, logger)
		// if ctx has a lower timeout, it will be used instead of DefaultTimeout
		// https://pkg.go.dev/context#WithDeadline
		ctx, cancel := context.WithTimeout(ctx, DefaultTimeout)
		err = l.connect(ctx)
		cancel()

		if err != nil {
			lastConnectError = err
			continue
		}

		e.locators = append(e.locators, l)
	}
	return lastConnectError
}

func (e *Environment) pickLocator(n int) *locator {
	if n > len(e.locators) {
		return e.locators[0]
	}
	return e.locators[n]
}

// # Public API

// CreateStream with name and given options.
func (e *Environment) CreateStream(ctx context.Context, name string, opts CreateStreamOptions) error {
	logger := raw.LoggerFromCtxOrDiscard(ctx)

	rn := rand.Intn(100)
	n := len(e.locators)

	var lastError error
	for i := 0; i < n; i++ {
		l := e.pickLocator((i + rn) % n)

		if err := l.maybeInitializeLocator(); err != nil {
			logger.Error("locator not available", slog.Any("error", err))
			lastError = err
			continue
		}

		// TODO: refactor to use maybeApplyDefaultTimeout()
		ctxCreate, cancel := context.WithTimeout(ctx, DefaultTimeout)
		// context cancellation is checked in the raw layer
		res := l.locatorOperation((*locator).operationCreateStream, ctxCreate, name, streamOptionsToRawStreamConfiguration(opts))
		cancel()

		// check for nil first, otherwise type assertion will panic
		if res[0] == nil {
			return nil
		}
		lastError = res[0].(error)

		// give up on non-retryable errors
		if isNonRetryableError(lastError) {
			return lastError
		}
	}

	return fmt.Errorf("locator operation failed: %w", lastError)
}

// DeleteStream with given name. Returns an error if the stream does not exist, or if any
// unknown error occurs. The context may carry a [slog.Logger] to log operations and
// intermediate errors, if any.
//
// See also: [raw.NewContextWithLogger]
func (e *Environment) DeleteStream(ctx context.Context, name string) error {
	logger := raw.LoggerFromCtxOrDiscard(ctx)

	rn := rand.Intn(100)
	n := len(e.locators)

	var lastError error
	var l *locator
	for i := 0; i < n; i++ {
		if e.locatorSelectSequential {
			// round robin / sequential
			l = e.locators[i]
		} else {
			// pick at random
			l = e.pickLocator((i + rn) % n)
		}

		if err := l.maybeInitializeLocator(); err != nil {
			logger.Error("locator not available", slog.Any("error", err))
			lastError = err
			continue
		}

		opCtx, cancel := maybeApplyDefaultTimeout(ctx)
		result := l.locatorOperation((*locator).operationDeleteStream, opCtx, name)
		if cancel != nil {
			cancel()
		}

		if result[0] == nil {
			return nil
		}

		if err := result[0].(error); err != nil {
			lastError = err
			if isNonRetryableError(lastError) {
				return lastError
			}
			logger.Error("locator operation failed", slog.Any("error", lastError))
		}
	}
	return lastError
}

// Close the connection to RabbitMQ server. This function closes all connections
// to RabbitMQ gracefully. A graceful disconnection sends a close request to RabbitMQ
// and awaits a confirmation response. If there's any error closing a connection,
// the error is logged to a logger extracted from the context.
func (e *Environment) Close(ctx context.Context) {
	//logger := raw.LoggerFromCtxOrDiscard(ctx).WithGroup("close")
	// TODO: shutdown producers/consumers
	for _, l := range e.locators {
		l.close()
	}
}

// QueryStreamStats queries the server for Stats from a given stream name.
// Stats available are 'first offset id' and 'committed chunk id'
//
// This command is available in RabbitMQ 3.11+
func (e *Environment) QueryStreamStats(ctx context.Context, name string) (Stats, error) {
	logger := raw.LoggerFromCtxOrDiscard(ctx)
	rn := rand.Intn(100)
	n := len(e.locators)

	var lastError error
	var l *locator
	for i := 0; i < n; i++ {
		if e.locatorSelectSequential {
			// round robin / sequential
			l = e.locators[i]
		} else {
			// pick at random
			l = e.pickLocator((i + rn) % n)
		}

		if err := l.maybeInitializeLocator(); err != nil {
			lastError = err
			logger.Error("error initializing locator", slog.Any("error", err))
			continue
		}

		result := l.locatorOperation((*locator).operationQueryStreamStats, ctx, name)
		if result[1] != nil {
			lastError = result[1].(error)
			if isNonRetryableError(lastError) {
				return Stats{-1, -1}, lastError
			}
			logger.Error("locator operation failed", slog.Any("error", lastError))
			continue
		}

		stats := result[0].(map[string]int64)
		return Stats{stats["first_chunk_id"], stats["committed_chunk_id"]}, nil
	}
	return Stats{-1, -1}, lastError
}

// QueryOffset retrieves the last consumer offset stored for a given consumer
// name and stream name.
func (e *Environment) QueryOffset(ctx context.Context, consumer, stream string) (uint64, error) {
	logger := raw.LoggerFromCtxOrDiscard(ctx)
	rn := rand.Intn(100)
	n := len(e.locators)

	var lastError error
	var l *locator
	for i := 0; i < n; i++ {
		if e.locatorSelectSequential {
			// round robin / sequential
			l = e.locators[i]
		} else {
			// pick at random
			l = e.pickLocator((i + rn) % n)
		}

		if err := l.maybeInitializeLocator(); err != nil {
			lastError = err
			logger.Error("error initializing locator", slog.Any("error", err))
			continue
		}

		result := l.locatorOperation((*locator).operationQueryOffset, ctx, consumer, stream)
		if result[1] != nil {
			lastError = result[1].(error)
			if isNonRetryableError(lastError) {
				return uint64(0), lastError
			}
			logger.Error("locator operation failed", slog.Any("error", lastError))
			continue
		}

		offset := result[0].(uint64)
		return offset, nil
	}
	return uint64(0), lastError
}

// QueryPartitions returns a list of partition streams for a given superstream name
func (e *Environment) QueryPartitions(ctx context.Context, superstream string) ([]string, error) {
	logger := raw.LoggerFromCtxOrDiscard(ctx)
	rn := rand.Intn(100)
	n := len(e.locators)

	var lastError error
	var l *locator
	for i := 0; i < n; i++ {
		if e.locatorSelectSequential {
			// round robin / sequential
			l = e.locators[i]
		} else {
			// pick at random
			l = e.pickLocator((i + rn) % n)
		}

		if err := l.maybeInitializeLocator(); err != nil {
			lastError = err
			logger.Error("error initializing locator", slog.Any("error", err))
			continue
		}

		result := l.locatorOperation((*locator).operationPartitions, ctx, superstream)
		if result[1] != nil {
			lastError = result[1].(error)
			if isNonRetryableError(lastError) {
				return nil, lastError
			}
			logger.Error("locator operation failed", slog.Any("error", lastError))
			continue
		}

		partitions := result[0].([]string)
		return partitions, nil
	}
	return nil, lastError
}

// QuerySequence retrieves the last publishingID for a given producer
// (reference) and stream name.
func (e *Environment) QuerySequence(ctx context.Context, reference, stream string) (uint64, error) {
	logger := raw.LoggerFromCtxOrDiscard(ctx)
	rn := rand.Intn(100)
	n := len(e.locators)
	var lastError error

	if !validateStringParameter(reference) {
		lastError = fmt.Errorf("producer reference invalid: %s", reference)
		return uint64(0), lastError
	}

	var l *locator
	for i := 0; i < n; i++ {
		if e.locatorSelectSequential {
			// round robin / sequential
			l = e.locators[i]
		} else {
			// pick at random
			l = e.pickLocator((i + rn) % n)
		}
		if err := l.maybeInitializeLocator(); err != nil {
			lastError = err
			logger.Error("error initializing locator", slog.Any("error", err))
			continue
		}

		result := l.locatorOperation((*locator).operationQuerySequence, ctx, reference, stream)
		if result[1] != nil {
			lastError = result[1].(error)
			if isNonRetryableError(lastError) {
				return uint64(0), lastError
			}
			logger.Error("locator operation failed", slog.Any("error", lastError))
			continue
		}

		pubId := result[0].(uint64)
		return pubId, nil
	}

	return uint64(0), lastError
}

func (e *Environment) CreateProducer(ctx context.Context, stream string, opts *ProducerOptions) (Producer, error) {
	// TODO: calls the producer manager
	/*
		1. locate leader for stream
		2. check if a producer manager is connected to leader
		2a. if exists and have capacity, fetch it
		2b. if not exists or not have capacity, continue
		2c. if not found, create a new producer manager
		3. create producer using publisher manager
		4. return producer

		Make a deep copy of ProducerOptions because we are going to modify the 'stream' attribute
	*/
	logger := raw.LoggerFromCtxOrDiscard(ctx).WithGroup("CreateProducer")
	rn := rand.Intn(100)
	n := len(e.locators)

	// 1. locate leader for stream
	var (
		lastError error
		l         *locator
		metadata  *raw.MetadataResponse
	)
	for i := 0; i < n; i++ {
		if e.locatorSelectSequential {
			// round robin / sequential
			l = e.locators[i]
		} else {
			// pick at random
			l = e.pickLocator((i + rn) % n)
		}
		result := l.locatorOperation((*locator).operationQueryStreamMetadata, ctx, []string{stream})
		if result[1] != nil {
			lastError = result[1].(error)
			if isNonRetryableError(lastError) {
				return nil, lastError
			}
			logger.Error("locator operation failed", slog.Any("error", lastError))
			continue
		}
		metadata = result[0].(*raw.MetadataResponse)
		lastError = nil
		break
	}

	if lastError != nil {
		return nil, fmt.Errorf("locator operation failed: %w", lastError)
	}

	brokerLeader, err := findLeader(stream, metadata)
	if err != nil {
		return nil, err
	}

	// if there's an address resolver, always use the address resolver
	var pmClient raw.Clienter
	var rc *raw.ClientConfiguration
	if e.addressResolver != nil {
		rc = e.locators[0].rawClientConf.DeepCopy()
		rc.RabbitmqAddr.Host, rc.RabbitmqAddr.Port = e.addressResolver(rc.RabbitmqAddr.Host, rc.RabbitmqAddr.Port)
		// TODO max attempts variable, depending on number of stream replicas
		for i := 0; i < 10; i++ {
			client, err := raw.DialConfig(ctx, rc)
			if err != nil {
				logger.Warn("failed to dial",
					slog.String("host", rc.RabbitmqAddr.Host),
					slog.Int("port", rc.RabbitmqAddr.Port),
					slog.Any("error", err),
				)
			}
			connProps := client.(*raw.Client).ConnectionProperties()
			if connProps["advertised_host"] == brokerLeader.host && connProps["advertised_port"] == brokerLeader.port {
				pmClient = client
				break
			}

			logger.Debug("connected to rabbit, but not to desired node",
				slog.String("host", connProps["advertised_host"]),
				slog.String("port", connProps["advertised_port"]),
			)
			ctx2, cancel := maybeApplyDefaultTimeout(ctx)
			_ = client.Close(ctx2)
			cancel()
			time.Sleep(time.Second)
		}
		if pmClient == nil {
			panic("could not create a client for a new producer manager")
		}
	} else {
		rc = e.locators[0].rawClientConf.DeepCopy()
		// TODO: set connection name
		c, err := raw.DialConfig(ctx, rc)
		if err != nil {
			panic(err)
		}
		pmClient = c
	}

	pm := newProducerManagerWithClient(0, e.configuration, pmClient, rc)
	e.producerManagers = append(e.producerManagers, pm)
	o := opts.DeepCopy()
	o.stream = stream

	// FIXME: check producer manager capacity before selecting a PM
	return pm.createProducer(ctx, o)
}

func findLeader(stream string, metadata *raw.MetadataResponse) (hostPort, error) {
	var leaderRef uint16
	var found = false
	for _, streamMetadata := range metadata.StreamsMetadata() {
		if streamMetadata.StreamName() == stream {
			found = true
			leaderRef = streamMetadata.LeaderReference()
			break
		}
	}
	if !found {
		return hostPort{}, errors.New("stream leader not found")
	}

	for _, broker := range metadata.Brokers() {
		if broker.Reference() == leaderRef {
			return hostPort{host: broker.Host(), port: strconv.Itoa(int(broker.Port()))}, nil
		}
	}
	return hostPort{}, errors.New("broker hosting leader not found")
}
