package stream

import (
	"fmt"
	"log"
	"time"
)

func uShortExtractResponseCode(code uint16) uint16 {
	return code & 0b0111_1111_1111_1111
}

//func UIntExtractResponseCode(code int32) int32 {
//	return code & 0b0111_1111_1111_1111
//}

func uShortEncodeResponseCode(code uint16) uint16 {
	return code | 0b1000_0000_0000_0000
}

func waitCodeWithDefaultTimeOut(response *Response) error {
	return waitCodeWithTimeOut(response, defaultSocketCallTimeout)
}
func waitCodeWithTimeOut(response *Response, timeout time.Duration) error {
	select {
	case code := <-response.code:
		if code.id != responseCodeOk {
			return lookErrorCode(code.id)
		}
		return nil
	case <-time.After(timeout):
		logError("timeout %d ms - waiting Code, operation: %s", defaultSocketCallTimeout, response.commandDescription)
		return fmt.Errorf("timeout %d ms - waiting Code, operation: %s ", defaultSocketCallTimeout, response.commandDescription)
	}
}

var logLevel int8

func SetLevelInfo(value int8) {
	logLevel = value
}

const (
	INFO  = 0
	DEBUG = 1
)

func logInfo(message string, v ...interface{}) {
	log.Printf(fmt.Sprintf("[info] - %s", message), v...)
}

func logError(message string, v ...interface{}) {
	log.Printf(fmt.Sprintf("[error] - %s", message), v...)
}

func logDebug(message string, v ...interface{}) {
	if logLevel > INFO {
		log.Printf(fmt.Sprintf("[debug] - %s", message), v...)
	}
}

func logWarn(message string, v ...interface{}) {
	log.Printf(fmt.Sprintf("[warn] - %s", message), v...)
}
