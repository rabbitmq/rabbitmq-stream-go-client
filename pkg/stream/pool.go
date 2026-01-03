package stream

import "time"

// connectionParameters holds the parameters needed to establish a connection
// to a broker
type connectionParameters struct {
	connectionName    string
	broker            *Broker
	tcpParameters     *TCPParameters
	saslConfiguration *SaslConfiguration
	rpcTimeout        time.Duration
}
