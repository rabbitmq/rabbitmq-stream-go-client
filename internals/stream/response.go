package stream

import "io"

type ReaderResponse struct {
	SocketReader io.Reader
}

func (readerResponse ReaderResponse) handleResponse()  {
	response := Response{}
	response.ReadResponseFromStream(readerResponse.SocketReader)
}



