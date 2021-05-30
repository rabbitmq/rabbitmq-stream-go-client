package ha

import (
	"github.com/pkg/errors"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/logs"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
	"time"
)

type ReliableProducer struct {
	env              *stream.Environment
	producer         *stream.Producer
	exitOnError      bool
	isOpen           bool
	backoff          int
	name             string
	tryConnect       chan int
	resultConnection chan bool
	done             chan struct{}
}

func New(env *stream.Environment) *ReliableProducer {
	return &ReliableProducer{
		env:              env,
		producer:         nil,
		exitOnError:      false,
		isOpen:           false,
		backoff:          1,
		name:             "",
		tryConnect:       make(chan int, 1),
		resultConnection: make(chan bool),
		done:             make(chan struct{}),
	}
}

func (p *ReliableProducer) reconnectMonitor(streamName string) {
	go func() {
		select {
		case <-p.tryConnect:
			{

				producer, err := p.env.NewProducer(streamName, nil)
				if err != nil {
					logs.LogInfo("Producer on stream %s creation fails, retry in %d seconds, error: %s", streamName, p.backoff, err)
					p.backoffWait()
					p.resultConnection <- false
				} else {
					p.producer = producer
					p.producer = producer
					channelClose := producer.NotifyClose()
					p.handleClose(channelClose)
					p.resultConnection <- true
				}

			}

		case <-p.done:
			{
				return
			}
		}

	}()
}

func (p *ReliableProducer) Start(streamName string, producerName string) error {
	p.name = producerName
	producerOptions := stream.NewProducerOptions()
	producerOptions.Name = producerName
	p.reconnectMonitor(streamName)
	tentatives := 0
	for tentatives < 5 {
		p.tryConnect <- tentatives
		res := <-p.resultConnection
		if res {
			logs.LogInfo("Producer on stream %s connected", streamName)
			return nil
		}
		tentatives++
		p.reconnectMonitor(streamName)
	}

	return errors.New("Can't create new producer")
}

func (p *ReliableProducer) handleClose(channelClose stream.ChannelClose) {
	go func(chClose stream.ChannelClose) {
		event := <-channelClose
		p.isOpen = false
		if event.Command == stream.CommandDeletePublisher {
			logs.LogInfo("Producer %s closed on stream %s, cause: %s", event.Name, event.StreamName, event.Reason)
			return
		}

		logs.LogInfo("Producer %s closed on stream %s, cause: %s. Going to restart it in %d seconds",
			event.Name,
			event.StreamName,
			event.Reason, p.backoff)
		p.backoffWait()
		p.Start(p.producer.GetStreamName(), p.name)

	}(channelClose)

}

func (p *ReliableProducer) Close() error {
	p.done <- struct{}{}

	if p.isOpen == false {
		return nil
	}
	p.isOpen = true
	p.producer = nil
	err := p.producer.Close()
	if err != nil {
		return err
	}
	return nil
}

func (p *ReliableProducer) backoffWait() {
	time.Sleep(time.Duration(p.backoff) * time.Second)
	p.backoff = p.backoff * 2
	if p.backoff > 8 {
		p.backoff = 1
	}
}
