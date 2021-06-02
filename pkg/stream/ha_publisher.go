package stream

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/logs"
	"sync"
	"time"
)

type ReliableProducer struct {
	env            *Environment
	producer       *Producer
	exitOnError    bool
	isOpen         bool
	backoff        int
	name           string
	streamMetadata *StreamMetadata
	mutex          *sync.Mutex
}

func NewHAProducer(env *Environment) *ReliableProducer {
	return &ReliableProducer{
		env:         env,
		producer:    nil,
		exitOnError: false,
		isOpen:      false,
		backoff:     1,
		name:        "",
		mutex:       &sync.Mutex{},
	}
}

func (p *ReliableProducer) IsOpen() bool {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	return p.isOpen
}

func (p *ReliableProducer) reconnectMonitor(streamName string) error {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	client := newClient(p.name)
	client.broker = p.streamMetadata.Leader
	err := client.connect()
	tentatives := 1
	for err != nil {
		logs.LogError("Can't connect the locator client, for stream %s error:%s, retry in %d seconds ", streamName, err, tentatives)
		time.Sleep(time.Duration(tentatives) * time.Second)
		err = client.connect()
		if len(p.streamMetadata.replicas) >= tentatives {
			client.broker = p.streamMetadata.replicas[tentatives]
		} else {
			logs.LogError("Can't connect the locator client, for stream %s error:%s ", streamName, err)
			return errors.New(fmt.Sprintf("Can't connect the locator client, for stream %s error:%s ", streamName, err))
		}
	}

	chMeta := make(chan metaDataUpdateEvent)
	client.metadataListener = chMeta
	go func(ch <-chan metaDataUpdateEvent, cl *Client) {
		for {
			<-ch
			p.mutex.Lock()
			p.isOpen = false
			p.mutex.Unlock()
			time.Sleep(10 * time.Millisecond)

			if cl.StreamExists(p.producer.GetStreamName()) {
				p.NewProducer(p.producer.GetStreamName(), p.name)
			}

			err := cl.Close()
			if err != nil {
				return
			}

			if !cl.socket.isOpen() {
				return
			}
		}

	}(chMeta, client)

	producer, err := client.DeclarePublisher(streamName, NewProducerOptions().SetProducerName(p.name))
	if err != nil {
		logs.LogWarn("Producer on stream %s creation fails, retry in %d seconds, error: %s", streamName, p.backoff, err)
		return err
	}
	p.isOpen = true
	p.producer = producer
	channelClose := producer.NotifyClose()
	p.handleClose(channelClose)
	logs.LogDebug("Producer connected on stream %s", streamName)
	p.backoff = 1

	return nil
}

func (p *ReliableProducer) GetConnectedBroker() *Broker {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	return p.producer.options.client.broker
}
func (p *ReliableProducer) NewProducer(streamName string, producerName string) error {
	p.name = producerName
	producerOptions := NewProducerOptions()
	producerOptions.Name = producerName
	data, err := p.env.StreamMetaData(streamName)
	if err != nil {
		return err
	}

	p.streamMetadata = data
	p.streamMetadata.Leader.cloneFrom(p.env.options.ConnectionParameters[0])
	for _, replica := range p.streamMetadata.replicas {
		replica.cloneFrom(p.env.options.ConnectionParameters[0])
	}

	tentatives := 0
	for tentatives < 3 {
		tentatives++
		err := p.reconnectMonitor(streamName)
		p.backoffWait()
		if err == nil {
			return nil
		}
	}
	return errors.New(fmt.Sprintf("Can't connect the locator client, for stream %s ", streamName))
}

func (p *ReliableProducer) handleClose(channelClose ChannelClose) {
	go func(chClose ChannelClose) {
		event := <-channelClose
		if event.Command == CommandDeletePublisher {
			p.isOpen = false
			logs.LogInfo("Producer %s closed on stream %s, cause: %s", event.Name, event.StreamName, event.Reason)
			return
		}
		if p.isOpen {
			logs.LogInfo("Producer %s closed on stream %s, cause: %s. Going to restart it in %d seconds",
				event.Name,
				event.StreamName,
				event.Reason, p.backoff)
			p.backoffWait()
			p.NewProducer(p.producer.GetStreamName(), p.name)
		}

	}(channelClose)

}

func (p *ReliableProducer) Close() error {
	if !p.isOpen {
		return nil
	}
	p.isOpen = false
	err := p.producer.options.client.deletePublisher(p.producer.ID)
	if err != nil {
		return err
	}
	err = p.producer.options.client.Close()
	if err != nil {
		return err
	}
	p.producer = nil
	return nil
}

func (p *ReliableProducer) backoffWait() {
	time.Sleep(time.Duration(p.backoff) * time.Second)
	p.backoff = p.backoff * 2
	if p.backoff > 8 {
		p.backoff = 1
	}
}
