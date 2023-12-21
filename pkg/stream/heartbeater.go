package stream

import (
	"github.com/rabbitmq/rabbitmq-stream-go-client/v2/pkg/raw"
	"golang.org/x/exp/slog"
	"sync"
	"time"
)

type heartBeater struct {
	logger       *slog.Logger
	client       raw.Clienter
	tickDuration time.Duration
	ticker       *time.Ticker
	done         *DoneChan
	receiveCh    <-chan *raw.Heartbeat
}

type DoneChan struct {
	C      chan struct{}
	closed bool
	mutex  sync.Mutex
}

func NewDoneChan() *DoneChan {
	return &DoneChan{C: make(chan struct{})}
}

// GracefulClose closes the DoneChan only if the Done chan is not already closed.
func (dc *DoneChan) GracefulClose() {
	dc.mutex.Lock()
	defer dc.mutex.Unlock()

	if !dc.closed {
		close(dc.C)
		dc.closed = true
	}
}

func NewHeartBeater(duration time.Duration, client raw.Clienter, logger *slog.Logger) *heartBeater {
	return &heartBeater{
		logger:       logger,
		client:       client,
		tickDuration: duration,
		done:         NewDoneChan(),
	}
}

func (hb *heartBeater) start() {
	hb.ticker = time.NewTicker(hb.tickDuration)
	hb.receiveCh = hb.client.NotifyHeartbeat()

	go func() {
		for {
			select {
			case <-hb.done.C:
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
	hb.done.GracefulClose()
}

func (hb *heartBeater) send() {
	err := hb.client.SendHeartbeat()
	if err != nil {
		hb.logger.Error("error sending heartbeat", "error", err)
	}
}
