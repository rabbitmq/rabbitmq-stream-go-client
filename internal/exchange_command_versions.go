package internal

import (
	"bufio"
)

var commandsInitiatedServerSide = []commandInformer{
	&PublishConfirmResponse{},
	// TODO: PublishError frame
	&ChunkResponse{},
	// TODO: MetadataUpdate frame
	&CloseRequest{},
}

type ExchangeCommandVersionsRequest struct {
	correlationId uint32
	commands      []commandInformer
}

func NewExchangeCommandVersionsRequest(correlationId uint32) *ExchangeCommandVersionsRequest {
	return &ExchangeCommandVersionsRequest{correlationId: correlationId, commands: commandsInitiatedServerSide}
}

func NewExchangeCommandVersionsRequestWith(correlationId uint32, commands []commandInformer) *ExchangeCommandVersionsRequest {
	return &ExchangeCommandVersionsRequest{
		correlationId: correlationId,
		commands:      commands,
	}
}

func (e *ExchangeCommandVersionsRequest) Write(w *bufio.Writer) (int, error) {
	nn, err := writeMany(w, e.correlationId, len(e.commands))
	if err != nil {
		return nn, err
	}
	for _, command := range e.commands {
		n, err := writeMany(w, command.Key(), command.MinVersion(), command.MaxVersion())
		nn += n
		if err != nil {
			return nn, err
		}
	}
	return nn, nil
}

func (e *ExchangeCommandVersionsRequest) Key() uint16 {
	return CommandExchangeCommandVersions
}

func (e *ExchangeCommandVersionsRequest) SizeNeeded() int {
	// key ID + Version + CorrelationID + slice len + (4 bytes for each element in slice)
	return streamProtocolHeaderSizeBytes + streamProtocolCorrelationIdSizeBytes + streamProtocolSliceLenBytes +
		(len(e.commands) * 6)
}

func (e *ExchangeCommandVersionsRequest) Version() int16 {
	return Version1
}

func (e *ExchangeCommandVersionsRequest) SetCorrelationId(id uint32) {
	e.correlationId = id
}

func (e *ExchangeCommandVersionsRequest) CorrelationId() uint32 {
	return e.correlationId
}

// commandInformation is used to decode the exchange command versions response
// it implements commandInformer, and it holds the min-max versions of a command
type commandInformation struct {
	key                    uint16
	minVersion, maxVersion int16
}

func (c *commandInformation) Key() uint16 {
	return c.key
}

func (c *commandInformation) MinVersion() int16 {
	return c.minVersion
}

func (c *commandInformation) MaxVersion() int16 {
	return c.maxVersion
}

// ExchangeCommandVersionsResponse is used to decode a response from the server
// to ExchangeCommandVersionsRequest
type ExchangeCommandVersionsResponse struct {
	correlationId uint32
	responseCode  uint16
	commands      []commandInformer
}

func (e *ExchangeCommandVersionsResponse) Read(r *bufio.Reader) error {
	var sliceLen uint32
	err := readMany(r, &e.correlationId, &e.responseCode, &sliceLen)
	if err != nil {
		return err
	}

	if sliceLen == 0 {
		return nil
	}
	e.commands = make([]commandInformer, sliceLen)

	for i := uint32(0); i < sliceLen; i++ {
		var key uint16
		var min, max int16
		err = readMany(r, &key, &min, &max)
		if err != nil {
			return err
		}
		e.commands[i] = &commandInformation{key, min, max}
	}

	return nil
}

func (e *ExchangeCommandVersionsResponse) CorrelationId() uint32 {
	return e.correlationId
}

func (e *ExchangeCommandVersionsResponse) ResponseCode() uint16 {
	return e.responseCode
}
