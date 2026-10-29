package stream

import (
	"sync"
	"testing"
)

func confirmationTestProducer() *Producer {
	return &Producer{confirmMutex: &sync.Mutex{}}
}

func requireClosedConfirmationChannel(t *testing.T, channel ChannelPublishConfirm) {
	t.Helper()
	select {
	case _, ok := <-channel:
		if ok {
			t.Fatal("confirmation channel is still open")
		}
	default:
		t.Fatal("confirmation channel was not closed")
	}
}

func TestConfirmationRegistrationAfterClose(t *testing.T) {
	p := confirmationTestProducer()
	p.closeConfirmationStatus()
	channel := p.NotifyPublishConfirmation()
	requireClosedConfirmationChannel(t, channel)
	if channel != p.NotifyPublishConfirmation() {
		t.Fatal("registration replaced the channel")
	}
	p.closeConfirmationStatus()
	p.sendConfirmationStatus(nil)
}

func TestConfirmationRegistrationPreservesPendingDelivery(t *testing.T) {
	p := confirmationTestProducer()
	channel := p.NotifyPublishConfirmation()
	status := &ConfirmationStatus{}
	p.sendConfirmationStatus([]*ConfirmationStatus{status})
	if channel != p.NotifyPublishConfirmation() {
		t.Fatal("registration discarded the existing receiver")
	}
	p.closeConfirmationStatus()
	select {
	case got, ok := <-channel:
		if !ok || len(got) != 1 || got[0] != status {
			t.Fatal("pending confirmation lost during close")
		}
	default:
		t.Fatal("pending confirmation not delivered")
	}
	requireClosedConfirmationChannel(t, channel)
}

func TestConfirmationRegistrationDuringClose(t *testing.T) {
	const iterations = 1000
	for i := 0; i < iterations; i++ {
		p := confirmationTestProducer()
		start := make(chan struct{})
		var channel ChannelPublishConfirm
		var wg sync.WaitGroup
		wg.Add(3)
		go func() { defer wg.Done(); <-start; channel = p.NotifyPublishConfirmation() }()
		go func() { defer wg.Done(); <-start; p.closeConfirmationStatus() }()
		go func() { defer wg.Done(); <-start; p.closeConfirmationStatus() }()
		close(start)
		wg.Wait()
		requireClosedConfirmationChannel(t, channel)
	}
}

func TestConfirmationConcurrentRegistrationAndDelivery(t *testing.T) {
	p := confirmationTestProducer()
	const registrations = 32
	channels := make([]ChannelPublishConfirm, registrations)
	var wg sync.WaitGroup
	wg.Add(registrations)
	for i := range channels {
		go func(i int) { defer wg.Done(); channels[i] = p.NotifyPublishConfirmation() }(i)
	}
	wg.Wait()
	for _, ch := range channels {
		if ch != channels[0] {
			t.Fatal("concurrent registration replaced the channel")
		}
	}
	status := &ConfirmationStatus{}
	wg.Add(2)
	go func() { defer wg.Done(); p.sendConfirmationStatus([]*ConfirmationStatus{status}) }()
	go func() { defer wg.Done(); p.closeConfirmationStatus() }()
	wg.Wait()
	for range channels[0] {
	}
}
