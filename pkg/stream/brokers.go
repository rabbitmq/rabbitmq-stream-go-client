package stream

import (
	"fmt"
	"sync"
)

type Broker struct {
	Host     string
	Port     int
	User     string
	Vhost    string
	Uri      string
	Password string
}

func newBrokerDefault() Broker {
	return Broker{
		Host:     "localhost",
		Port:     StreamTcpPort,
		User:     "guest",
		Password: "guest",
		Vhost:    "/",
	}
}

func (br *Broker) mergeWithDefault() {
	broker := newBrokerDefault()
	if br.Host == "" {
		br.Host = broker.Host
	}
	if br.Vhost == "" {
		br.Vhost = broker.Vhost
	}

	if br.User == "" {
		br.User = broker.User
	}
	if br.User == "" {
		br.User = broker.User
	}
	if br.Password == "" {
		br.Password = broker.Password
	}
	if br.Port == 0 {
		br.Port = broker.Port
	}

}

func (br *Broker) cloneFrom(broker Broker) {
	br.User = broker.User
	br.Password = broker.Password
	br.Vhost = broker.Vhost

}

func (br *Broker) GetUri() string {
	if br.Uri == "" {
		br.Uri = fmt.Sprintf("rabbitmq-streaming://%s:%s@%s:%d/%s",
			br.User, br.Password,
			br.Host, br.Port, br.Vhost)
	}
	return br.Uri
}

func newBroker(host string, port uint32) *Broker {
	return &Broker{
		Host: host,
		Port: int(port),
	}
}

type Brokers struct {
	items *sync.Map
}

func newBrokers() *Brokers {
	return &Brokers{items: &sync.Map{}}
}

func (brs *Brokers) Add(brokerReference int16, host string, port uint32) *Broker {
	broker := newBroker(host, port)
	brs.items.Store(brokerReference, broker)
	return broker
}

func (brs *Brokers) Get(brokerReference int16) *Broker {
	value, ok := brs.items.Load(brokerReference)
	if !ok {
		return nil
	}

	return value.(*Broker)
}

func (br *Broker) hostPort() string {
	return fmt.Sprintf("%s:%d", br.Host, br.Port)
}

type StreamMetadata struct {
	stream       string
	responseCode uint16
	Leader       *Broker
	replicas     []*Broker
}

func (StreamMetadata) New(stream string, responseCode uint16,
	leader *Broker, replicas []*Broker) *StreamMetadata {
	return &StreamMetadata{stream: stream, responseCode: responseCode,
		Leader: leader, replicas: replicas}
}

type StreamsMetadata struct {
	items *sync.Map
}

func (StreamsMetadata) New() *StreamsMetadata {
	return &StreamsMetadata{&sync.Map{}}
}

func (smd *StreamsMetadata) Add(stream string, responseCode uint16,
	leader *Broker, replicas []*Broker) *StreamMetadata {
	streamMetadata := StreamMetadata{}.New(stream, responseCode,
		leader, replicas)
	smd.items.Store(stream, streamMetadata)
	return streamMetadata
}

func (smd *StreamsMetadata) Get(stream string) *StreamMetadata {
	value, ok := smd.items.Load(stream)
	if !ok {
		return nil
	}
	return value.(*StreamMetadata)
}
