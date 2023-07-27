package stream

import (
	"github.com/rabbitmq/rabbitmq-stream-go-client/v2/pkg/raw"
	"golang.org/x/exp/slog"
	"time"
)

type heartBeater struct {
	logger       *slog.Logger
	client       raw.Clienter
	tickDuration time.Duration
	ticker       *time.Ticker
	done         chan struct{}
	receiveCh    <-chan *raw.Heartbeat
}

func NewHeartBeater(duration time.Duration, client raw.Clienter, logger *slog.Logger) *heartBeater {
	return &heartBeater{
		logger:       logger,
		client:       client,
		tickDuration: duration,
		done:         make(chan struct{}),
	}
}

func (hb *heartBeater) start() {
	hb.ticker = time.NewTicker(hb.tickDuration)
	hb.receiveCh = hb.client.NotifyHeartbeat()

	go func() {
		for {
			select {
			case <-hb.done:
				return
			case <-hb.ticker.C:
				hb.send()
			case <-hb.receiveCh:
				hb.send()
			}
		}
	}()
}

func (hb *heartBeater) reset() {
	// This nil check is mainly for tests.
	if hb == nil || hb.ticker == nil {
		return
	}
	hb.ticker.Reset(hb.tickDuration)
}

func (hb *heartBeater) stop() {
	hb.ticker.Stop()
	close(hb.done)
}

func (hb *heartBeater) send() {
	err := hb.client.SendHeartbeat()
	if err != nil {
		hb.logger.Error("error sending heartbeat", "error", err)
	}
}
