package stream

import (
	"errors"
	"fmt"
	"time"
)

func UShortExtractResponseCode(code uint16) uint16 {
	return code & 0b0111_1111_1111_1111
}

func UIntExtractResponseCode(code int32) int32 {
	return code & 0b0111_1111_1111_1111
}

func UShortEncodeResponseCode(code uint16) uint16 {
	return code | 0b1000_0000_0000_0000
}

func WaitCodeWithDefaultTimeOut(response *Response, command int) (Code, error) {
	return WaitCodeWithTimeOut(response, command, DefaultSocketCallTimeout)
}
func WaitCodeWithTimeOut(response *Response, command int, timeout time.Duration) (Code, error) {
	select {
	case code := <-response.code:
		return code, nil
	case <-time.After(timeout):
		fmt.Printf("Timeout waiting Code, operation:%d \n", command)
		return Code{}, errors.New(fmt.Sprintf("Timeout waiting Code, operation:%d \n", command))
	}
}
