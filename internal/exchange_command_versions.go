package internal

import (
	"bufio"
)

var commandsInitiatedServerSide = []commandInformation{
	&PublishConfirmResponse{},
	// TODO: PublishError frame
	&ChunkResponse{},
	// TODO: MetadataUpdate frame
	&CloseRequest{},
}

type ExchangeCommandVersionsRequest struct {
	correlationId uint32
	commands      []commandInformation
}

func NewExchangeCommandVersionsRequest(correlationId uint32) *ExchangeCommandVersionsRequest {
	return &ExchangeCommandVersionsRequest{correlationId: correlationId, commands: commandsInitiatedServerSide}
}

func NewExchangeCommandVersionsRequestWith(correlationId uint32, commands []commandInformation) *ExchangeCommandVersionsRequest {
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
