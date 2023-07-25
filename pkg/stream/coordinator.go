package stream

import (
	"context"
	"errors"
	"github.com/rabbitmq/rabbitmq-stream-go-client/v2/pkg/raw"
	"golang.org/x/exp/slog"
	"sync"
)

var (
	errCoordinatorFull = errors.New("coordinator is at maximum capacity")
)

type producerCoordinator struct {
	mu                  sync.Mutex
	maxCapacity         int
	publisherIdSequence autoIncrementNumber[uint8]
	producers           map[int]Producer // TODO: is it ok to reuse publisher ID?
	producerMu          *sync.Mutex
	producerClient      raw.Clienter
	host                string
	port                int
}

//func (pc *producerCoordinator) nextAvailablePublisherId() (uint8, error) {
//	if len(pc.producers) == pc.maxCapacity {
//		return 0, errCoordinatorFull
//	}
//
//	return pc.publisherIdSequence.next(), nil
//}

func (pc *producerCoordinator) register(producerId int, p Producer) (uint8, error) {
	pc.mu.Lock()
	if len(pc.producers) == pc.maxCapacity {
		pc.mu.Unlock()
		return 0, errCoordinatorFull
	}
	pc.producers[producerId] = p
	pubId := pc.publisherIdSequence.next()
	pc.mu.Unlock()

	// FIXME check if client is open

	switch pp := p.(type) {
	case *smartProducer:
		pp.client = pc.producerClient
		pp.clientMu = pc.producerMu
	default:
		panic("unknown producer implementation")
	}

	// done "callback" to remove producer from this coordinator
	go pc.deregisterWhenDone(producerId, p)
	return pubId, nil
}

func (pc *producerCoordinator) deregisterWhenDone(producerId int, p Producer) {
	<-p.done()
	pc.mu.Lock()
	defer pc.mu.Unlock()
	delete(pc.producers, producerId)

	if len(pc.producers) == 0 {
		pc.producerMu.Lock()
		defer pc.producerMu.Unlock()
		_ = pc.producerClient.Close(context.TODO())
	}
}

func (pc *producerCoordinator) hasCapacity() bool {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	return len(pc.producers) < pc.maxCapacity
}

func (pc *producerCoordinator) Close(ctx context.Context) error {
	pc.mu.Lock()
	pc.producerMu.Lock()
	defer pc.mu.Unlock()
	defer pc.producerMu.Unlock()

	logger := raw.LoggerFromCtxOrDiscard(ctx).WithGroup("producerCoordinator")

	for _, producer := range pc.producers {
		err := producer.Close()
		if err != nil {
			logger.Error("error closing producer", slog.Int("producerId", producer.id()), slog.Any("error", err))
		}
	}
	ctx2, cancel := maybeApplyDefaultTimeout(ctx)
	defer cancel()
	return pc.producerClient.Close(ctx2)
}
