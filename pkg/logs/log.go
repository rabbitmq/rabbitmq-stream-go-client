package logs

import (
	"fmt"
	"log"
)

const (
	INFO  = 0
	DEBUG = 1
)

type Logger interface {
	Info(message string, v ...any)
	Error(message string, v ...any)
	Debug(message string, v ...any)
	Warn(message string, v ...any)
}

func SetLogger(l Logger) {
	logger = l
}

var logger Logger = defaultLogger{}

var LogLevel int8

func LogInfo(message string, v ...any) {
	logger.Info(message, v...)
}

func LogError(message string, v ...any) {
	logger.Error(message, v...)
}

func LogDebug(message string, v ...any) {
	logger.Debug(message, v...)
}

func LogWarn(message string, v ...any) {
	logger.Warn(message, v...)
}

type defaultLogger struct{}

func (l defaultLogger) Info(message string, v ...any) {
	log.Printf(fmt.Sprintf("[info] - %s", message), v...)
}

func (l defaultLogger) Error(message string, v ...any) {
	log.Printf(fmt.Sprintf("[error] - %s", message), v...)
}

func (l defaultLogger) Debug(message string, v ...any) {
	if LogLevel > INFO {
		log.Printf(fmt.Sprintf("[debug] - %s", message), v...)
	}
}

func (l defaultLogger) Warn(message string, v ...any) {
	log.Printf(fmt.Sprintf("[warn] - %s", message), v...)
}
