//go:build windows
// +build windows

package stream

import (
	"runtime"
	"syscall"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/logs"
)

var controlFunc func(network, address string, c syscall.RawConn) error

func init() {
	controlFunc = func(network, address string, c syscall.RawConn) error {
		return c.Control(func(fd uintptr) {
			err := syscall.SetsockoptInt(syscall.Handle(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF, defaultReadSocketBuffer)
			runtime.KeepAlive(fd)
			if err != nil {
				logs.LogError("Set socket option error: %s", err)
				return
			}

			err = syscall.SetsockoptInt(syscall.Handle(fd), syscall.SOL_SOCKET, syscall.SO_SNDBUF, defaultWriteSocketBuffer)
			runtime.KeepAlive(fd)
			if err != nil {
				logs.LogError("Set socket option error: %s", err)
				return
			}

		})
	}
}
