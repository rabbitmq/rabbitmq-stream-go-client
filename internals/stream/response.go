package stream

import "io"

type ReaderResponse struct {
	SocketReader io.Reader
	response     *Response
}

func (readerResponse ReaderResponse) handleResponse() {
	readerResponse.response = &Response{}
	readerResponse.response.ReadResponseFromStream(readerResponse.SocketReader)
	switch readerResponse.response.CommandID {

	case CommandPeerProperties:
		{
			readerResponse.handlePeerProperties()
		}
	}
}

func (readerResponse ReaderResponse) handlePeerProperties() {
	readerResponse.response.CorrelationId = ReadIntFromReader(readerResponse.SocketReader)
	readerResponse.response.ResponseCode = ReadShortFromReader(readerResponse.SocketReader)

	serverPropertiesCount := ReadIntFromReader(readerResponse.SocketReader)
	serverProperties := make(map[string]string)

	for i := 0; i < int(serverPropertiesCount); i++ {
		key := ReadStringFromReader(readerResponse.SocketReader)
		value := ReadStringFromReader(readerResponse.SocketReader)
		serverProperties[key] = value
	}

}
