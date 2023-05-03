package internal

import (
	"bufio"
	"bytes"
)

var commandsInitiatedServerSide = []commandInformer{
	&PublishConfirmResponse{},
	// TODO: PublishError frame
	&ChunkResponse{},
	&MetadataUpdateResponse{},
	&CloseRequest{},
}

type ExchangeCommandVersionsRequest struct {
	correlationId uint32
	commands      []commandInformer
}

func NewExchangeCommandVersionsRequest() *ExchangeCommandVersionsRequest {
	return &ExchangeCommandVersionsRequest{commands: commandsInitiatedServerSide}
}

func NewExchangeCommandVersionsRequestWith(correlationId uint32, commands []commandInformer) *ExchangeCommandVersionsRequest {
	return &ExchangeCommandVersionsRequest{
		correlationId: correlationId,
		commands:      commands,
	}
}

func (e *ExchangeCommandVersionsRequest) UnmarshalBinary(data []byte) error {
	b := bytes.NewReader(data)
	var n uint32
	err := readMany(b, &e.correlationId, &n)
	if err != nil {
		return err
	}

	if n == 0 {
		return nil
	}

	for i := uint32(0); i < n; i++ {
		commandInf := &commandInformation{}
		err = readMany(b, &commandInf.key, &commandInf.minVersion, &commandInf.maxVersion)
		if err != nil {
			return err
		}
	}
	return nil
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

func NewExchangeCommandVersionsResponse(id uint32, responseCode uint16, commands ...commandInformer) *ExchangeCommandVersionsResponse {
	e := &ExchangeCommandVersionsResponse{correlationId: id, responseCode: responseCode}
	e.commands = append(e.commands, commands...)
	return e
}

func (e *ExchangeCommandVersionsResponse) MarshalBinary() (data []byte, err error) {
	b := &bytes.Buffer{}
	nn, err := writeMany(b, &e.correlationId, &e.responseCode, len(e.commands))
	if err != nil {
		return nil, err
	}
	// correlation-id 4b + response-code 2b + slice-len 4b
	if nn != 10 {
		return nil, errWriteShort
	}

	for i := 0; i < len(e.commands); i++ {
		n, err := writeMany(b, e.commands[i].Key(), e.commands[i].MinVersion(), e.commands[i].MaxVersion())
		if err != nil {
			return nil, err
		}
		if n != 6 {
			return nil, errWriteShort
		}
	}
	data = b.Bytes()
	return
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
