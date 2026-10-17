package logging

import (
	"fmt"
	"log"
	"os"
	"runtime"
	"strings"

	"github.com/uc-cdis/arborist/arborist"
)

const (
	LogLevelDebug   arborist.LogLevel = "DEBUG"
	LogLevelInfo    arborist.LogLevel = "INFO"
	LogLevelWarning arborist.LogLevel = "WARNING"
	LogLevelError   arborist.LogLevel = "ERROR"
)

type Handler struct {
	Logger *log.Logger
}

func newHandler() *Handler {
	return &Handler{Logger: log.New(os.Stderr, "", log.Ldate|log.Ltime)}
}

func (handler *Handler) Print(format string, a ...any) {
	handler.Logger.Print(sprintf(format, a...))
}

func (handler *Handler) Debug(format string, a ...any) {
	handler.Logger.Print(logMsg(LogLevelDebug, format, a...))
}

func (handler *Handler) Info(format string, a ...any) {
	handler.Logger.Print(logMsg(LogLevelInfo, format, a...))
}

func (handler *Handler) Warning(format string, a ...any) {
	handler.Logger.Print(logMsg(LogLevelWarning, format, a...))
}

func (handler *Handler) Error(format string, a ...any) {
	handler.Logger.Print(logMsg(LogLevelError, format, a...))
}

func logMsg(lvl arborist.LogLevel, format string, a ...any) string {
	msg := sprintf(format, a...)
	msg = fmt.Sprintf("%s: %s", lvl, msg)
	// get the call from 2 stack frames above this
	// (one call up is the Cache method, so go one more above that)
	_, fn, line, ok := runtime.Caller(2)
	if ok {
		// shorten the filepath to only the basename
		split := strings.Split(fn, string(os.PathSeparator))
		fn = split[len(split)-1]
		msg = fmt.Sprintf("%s:%d: %s", fn, line, msg)
	}
	return msg
}

func sprintf(format string, a ...any) string {
	if len(a) == 0 {
		return format
	}
	return fmt.Sprintf(format, a...)
}
