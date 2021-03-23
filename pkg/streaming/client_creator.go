package streaming

type ClientCreator struct {
	client *Client
	uri    string
}

func NewClientCreator() *ClientCreator {
	client := &Client{
		producers: NewProducers(),
		responses: NewResponses(),
		consumers: NewConsumers(),
	}
	return &ClientCreator{client: client}
}

func (cc *ClientCreator) Uri(uri string) *ClientCreator {
	cc.uri = uri
	return cc
}

func (cc *ClientCreator) Connect() (*Client, error) {
	if cc.uri == "" {
		cc.uri = LocalhostUriConnection
	}
	res := cc.client.connect(cc.uri)
	return cc.client, res
}
