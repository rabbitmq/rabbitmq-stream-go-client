package stream

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
