package internal

import (
	"bufio"
	"net"
)

type Connecter interface {
	GetWriter() *bufio.Writer
	GetReader() *bufio.Reader
}

type Connection struct {
	connection net.Conn
	writer     *bufio.Writer
	reader     *bufio.Reader
}

func NewConnection(connection net.Conn) *Connection {
	return &Connection{connection: connection,
		reader: bufio.NewReader(connection),
		writer: bufio.NewWriter(connection)}
}

func (c *Connection) GetWriter() *bufio.Writer {
	return c.writer
}

func (c *Connection) GetReader() *bufio.Reader {
	return c.reader
}
