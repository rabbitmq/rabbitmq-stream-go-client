package stream

import "context"

type Clienter interface {
	Connect(ctx context.Context, brokers []Broker) error
	DeclareStream(ctx context.Context, name string) error
}

type Broker struct {
	Host     string
	Port     string
	User     string
	Vhost    string
	Uri      string
	Password string
	Scheme   string

	advHost string
	advPort string
}
