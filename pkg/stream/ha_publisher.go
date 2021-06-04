package stream

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/logs"
	"net"
	"sync"
	"time"
)

const (
	StatusOpen               = 1
	StatusClosed             = 2
	StatusReconnection       = 3
	StatusStreamDoesNotExist = 4
)

type ReliableProducer struct {
	env                      *Environment
	producer                 *Producer
	exitOnError              bool
	status                   int
	backoff                  int
	name                     string
	streamMetadata           *StreamMetadata
	mutex                    *sync.Mutex
	publishChannel           chan []*amqp.Message
	resendUnConfirmedMessage bool
}

func NewHAProducer(env *Environment) *ReliableProducer {
	res := &ReliableProducer{
		env:                      env,
		producer:                 nil,
		exitOnError:              false,
		status:                   StatusClosed,
		backoff:                  1,
		name:                     "",
		mutex:                    &sync.Mutex{},
		publishChannel:           make(chan []*amqp.Message, 100000),
		resendUnConfirmedMessage: false,
	}
	res.asyncPublish()
	return res
}

func (p *ReliableProducer) IsOpen() bool {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	return p.status == StatusOpen
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
		for range ch {
			p.mutex.Lock()
			p.status = StatusReconnection
			p.mutex.Unlock()
			time.Sleep(10 * time.Millisecond)

			if cl.StreamExists(p.producer.GetStreamName()) {
				p.NewProducer(p.producer.GetStreamName(), p.name)
			} else {
				p.status = StatusStreamDoesNotExist
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

	var producer *Producer
	var errGetProducer error
	if p.producer == nil {
		producer, errGetProducer = client.DeclarePublisher(streamName, NewProducerOptions().SetProducerName(p.name))
	} else {
		producer, errGetProducer = client.ReusePublisher(streamName, p.producer)
	}
	if errGetProducer != nil {
		logs.LogWarn("Producer on stream %s creation fails, retry in %d seconds, error: %s", streamName, p.backoff, err)
		return errGetProducer
	}
	p.status = StatusOpen
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
			p.status = StatusReconnection
			logs.LogInfo("Producer %s closed on stream %s, cause: %s", event.Name, event.StreamName, event.Reason)
			return
		}
		if p.IsOpen() {
			logs.LogInfo("Producer %s closed on stream %s, cause: %s. Going to restart it in %d seconds",
				event.Name,
				event.StreamName,
				event.Reason, p.backoff)
			p.backoffWait()
			p.NewProducer(p.producer.GetStreamName(), p.name)
		}

	}(channelClose)
}

func (p *ReliableProducer) publishOldUnConfirmed() {
	if p.resendUnConfirmedMessage {
		err := p.producer.ResendUnConfirmed(context.TODO())
		if err != nil {
			return
		}
	}
	p.resendUnConfirmedMessage = false
}

func (p *ReliableProducer) asyncPublish() {
	go func() {
		for messages := range p.publishChannel {
			p.publishOldUnConfirmed()
			_, errW := p.producer.BatchPublish(context.TODO(), messages)
			switch err := errW.(type) {
			case *net.OpError:
				fmt.Printf("cant %s \n", err.Err)
				time.Sleep(200 * time.Millisecond)
				p.resendUnConfirmedMessage = true
			}
		}
	}()

}

func (p *ReliableProducer) BatchPublish(msgs []*amqp.Message) error {
	if p.status == StatusStreamDoesNotExist {
		return StreamDoesNotExist
	}
	if p.status == StatusClosed {
		return errors.New("Producer is closed")
	}

	p.publishChannel <- msgs
	return nil
	//_, err := p.producer.BatchPublish(context.TODO(), msgs)
	//if err != nil {
	//	return
	//}
}

func (p *ReliableProducer) NotifyPublishError() ChannelPublishError {
	return p.producer.NotifyPublishError()
}

func (p *ReliableProducer) NotifyPublishConfirmation() ChannelPublishConfirm {
	return p.producer.NotifyPublishConfirmation()

}

func (p *ReliableProducer) Close() error {
	if !p.IsOpen() {
		return nil
	}
	p.status = StatusClosed

	err := p.producer.options.client.deletePublisher(p.producer.ID)
	if err != nil {
		return err
	}
	err = p.producer.options.client.Close()
	if err != nil {
		return err
	}
	close(p.publishChannel)
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
