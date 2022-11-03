package internal

import (
	"bufio"
	"net"
	"time"
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

func (c *Connection) SetDeadline(t time.Time) error {
	return c.connection.SetDeadline(t)
}

func (c *Connection) Close() error {
	return c.connection.Close()
}
