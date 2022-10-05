package stream

import (
	"github.com/gsantomaggio/rabbitmq-stream-go-client/internal"
	"net"
	"sync"
	"sync/atomic"
)

type correlation struct {
	id         uint32
	chResponse chan internal.CommandRead
}

func NewCorrelation(id uint32) *correlation {
	return &correlation{chResponse: make(chan internal.CommandRead),
		id: id}
}

func (c *correlation) Close() {
	close(c.chResponse)
}

type TCPClient struct {
	connection      *internal.Connection
	correlationsMap sync.Map
	nextCorrelation uint32
}

func NewTcpClient(connection net.Conn) Clienter {
	tcp := &TCPClient{connection: internal.NewConnection(connection), correlationsMap: sync.Map{}}
	go tcp.handleIncoming()
	return tcp
}

// correlation map section

func (tc *TCPClient) getCorrelationById(id uint32) *correlation {
	if v, ok := tc.correlationsMap.Load(id); ok {
		return v.(*correlation)
	}
	return nil
}
func (tc *TCPClient) storeCorrelation(request internal.CommandWrite) {
	request.SetCorrelationId(tc.getNextCorrelation())
	tc.correlationsMap.Store(request.CorrelationId(), NewCorrelation(request.CorrelationId()))
}

func (tc *TCPClient) removeCorrelation(id uint32) {
	tc.getCorrelationById(id).Close()
	tc.correlationsMap.Delete(id)
}

// end correlation map section

func (tc *TCPClient) writeCommand(request internal.CommandWrite) error {
	hWritten, err := internal.NewHeaderRequest(request).Write(tc.connection.GetWriter())
	if err != nil {
		return err
	}
	bWritten, err := request.Write(tc.connection.GetWriter())
	if err != nil {
		return err
	}
	if (bWritten + hWritten) != (request.SizeNeeded() + 4) {
		panic("Write Command: Not all bytes written")
	}
	return tc.connection.GetWriter().Flush()
}

func (tc *TCPClient) request(request internal.CommandWrite) (internal.CommandRead, error) {
	tc.storeCorrelation(request)
	defer tc.removeCorrelation(request.CorrelationId())
	err := tc.writeCommand(request)
	if err != nil {
		return nil, err
	}
	select {
	case r := <-tc.getCorrelationById(request.CorrelationId()).chResponse:
		return r, nil
	}
}

func (tc *TCPClient) getNextCorrelation() uint32 {
	return atomic.AddUint32(&tc.nextCorrelation, 1)
}

func (tc *TCPClient) handleResponse(read internal.CommandRead) {
	tc.getCorrelationById(read.CorrelationId()).chResponse <- read
}

func (tc *TCPClient) handleIncoming() {
	buffer := tc.connection.GetReader()
	for {
		var header = new(internal.Header)
		err := header.Read(buffer)
		if err != nil {
			panic(err)
		}
		switch internal.UShortExtractResponseCode(header.Command()) {
		case internal.CommandPeerProperties:
			peerPropResponse := internal.NewPeerPropertiesResponse()
			err = peerPropResponse.Read(buffer)
			if err != nil {
				panic(err)
			}
			tc.handleResponse(peerPropResponse)
			break
		case internal.CommandSaslMechanisms:
			saslMechanismsResponse := internal.NewSaslMechanismsResponse()
			err = saslMechanismsResponse.Read(buffer)
			if err != nil {
				panic(err)
			}
			tc.handleResponse(saslMechanismsResponse)
			break
		default:
			panic("unknown command")
		}
	}
}

func (tc *TCPClient) peerProperties() error {
	serverProperties, err := tc.request(internal.NewPeerPropertiesRequest())
	internal.Debug("peerPropertiesResponse: %v", serverProperties.(*internal.PeerPropertiesResponse).ServerProperties)
	return err
}

func (tc *TCPClient) saslMechanisms() error {
	saslMechanisms, err := tc.request(internal.NewSaslMechanismsRequest())
	internal.Debug("saslMechanismsResponse: %v", saslMechanisms.(*internal.SaslMechanismsResponse).Mechanisms)
	return err

}

// public API

func (tc *TCPClient) Connect(brokers []Broker) error {
	err := tc.peerProperties()
	if internal.MaybeLogError(err, "error reading server properties") {
		return err
	}
	err = tc.saslMechanisms()
	if internal.MaybeLogError(err, "error reading sasl mechanisms") {
		return err
	}

	return nil
}
