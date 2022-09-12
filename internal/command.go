package internal

import (
	"bufio"
)

type CommandRead interface {
	Read(reader *bufio.Reader)
	GetCorrelationId() uint32
}

type CommandWrite interface {
	Write(writer *bufio.Writer) (int, error)
	GetKey() uint16
	SizeNeeded() int
	SetCorrelationId(id uint32)
	GetCorrelationId() uint32
}
