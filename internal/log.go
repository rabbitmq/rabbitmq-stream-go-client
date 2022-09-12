package internal

import (
	"fmt"
	"log"
)

const (
	INFO  = 0
	DEBUG = 1
)

var LogLevel int8

func Info(message string, v ...interface{}) {
	log.Printf(fmt.Sprintf("[info] - %s", message), v...)
}

func Error(message string, v ...interface{}) {
	log.Printf(fmt.Sprintf("[error] - %s", message), v...)
}

func Debug(message string, v ...interface{}) {
	if LogLevel > INFO {
		log.Printf(fmt.Sprintf("[debug] - %s", message), v...)
	}
}

func Warn(message string, v ...interface{}) {
	log.Printf(fmt.Sprintf("[warn] - %s", message), v...)
}

func SetLevelInfo(value int8) {
	LogLevel = value
}

func MaybeLogError(err error, message ...string) bool {
	if err != nil {
		Error(fmt.Sprintf("%s %v", err, message))
	}
	return err != nil
}
