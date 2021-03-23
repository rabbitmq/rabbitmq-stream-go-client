package streaming

import (
	"errors"
	"fmt"
	"time"
)

func UShortExtractResponseCode(code uint16) uint16 {
	return code & 0b0111_1111_1111_1111
}

//func UIntExtractResponseCode(code int32) int32 {
//	return code & 0b0111_1111_1111_1111
//}

func UShortEncodeResponseCode(code uint16) uint16 {
	return code | 0b1000_0000_0000_0000
}

func WaitCodeWithDefaultTimeOut(response *Response) error {
	return WaitCodeWithTimeOut(response, DefaultSocketCallTimeout)
}
func WaitCodeWithTimeOut(response *Response, timeout time.Duration) error {
	select {
	case code := <-response.code:
		if code.id != ResponseCodeOk {
			return errors.New(fmt.Sprintf("Error: %s", LookErrorCode(code.id)))
		}
		return nil
	case <-time.After(timeout):
		fmt.Printf("Timeout waiting Code, operation:%d \n", response.subId)
		return errors.New(fmt.Sprintf("Timeout waiting Code, operation:%d \n", response.subId))
	}
}

func CheckErr(err error) {
	if err != nil {
		fmt.Printf("Error operation: %s \n", err)
	}
}
