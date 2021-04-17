package streaming

import (
	"fmt"
	"log"
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
			return fmt.Errorf("wait time error: %s", LookErrorCode(code.id))
		}
		return nil
	case <-time.After(timeout):
		WARN("timeout waiting Code, operation:%d", response.correlationid)
		return fmt.Errorf("timeout waiting Code, operation:%d ", response.correlationid)
	}
}

// logging

func INFO(message string, v ...interface{}) {
	log.Printf(fmt.Sprintf("[INFO] - %s", message), v...)
}

func ERROR(message string, v ...interface{}) {
	log.Printf(fmt.Sprintf("[ERROR] - %s", message), v...)
}

func DEBUG(message string, v ...interface{}) {
	log.Printf(fmt.Sprintf("[DEBUG] - %s", message), v...)
}

func WARN(message string, v ...interface{}) {
	log.Printf(fmt.Sprintf("[WARN] - %s", message), v...)
}
