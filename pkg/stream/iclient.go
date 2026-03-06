package stream

// IClient is the interface for the Client struct, defining the public API
// for stream operations.
type IClient interface {
	Close()
	DeleteStream(streamName string) error
	DeclarePublisher(streamName string, options *ProducerOptions) (*Producer, error)
	BrokerLeader(stream string) (*Broker, error)
	BrokerLeaderWithResolver(stream string, resolver *AddressResolver) (*Broker, error)
	StreamExists(stream string) bool
	BrokerForConsumer(stream string) (*Broker, error)
	DeclareStream(streamName string, options *StreamOptions) error
	StoreOffset(consumerName string, streamName string, offset int64) error
	DeclareSubscriber(streamName string, messagesHandler MessagesHandler, options *ConsumerOptions) (*Consumer, error)
	StreamStats(streamName string) (*StreamStats, error)
	DeclareSuperStream(superStream string, options SuperStreamOptions) error
	DeleteSuperStream(superStream string) error
	QueryPartitions(superStream string) ([]string, error)
}
