package gecko

import (
	"fmt"
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

type LogCache struct {
	logs []Log
}

type Log struct {
	lvl arborist.LogLevel
	msg string
}

func (cache *LogCache) write(logger arborist.Logger) {
	if l, ok := logger.(*LogHandler); ok {
		for _, log := range cache.logs {
			switch log.lvl {
			case LogLevelDebug:
				l.Debug("%s", log.msg)
			case LogLevelInfo:
				l.Info("%s", log.msg)
			case LogLevelWarning:
				l.Warning("%s", log.msg)
			case LogLevelError:
				l.Error("%s", log.msg)
			default:
				l.Print("%s", log.msg)
			}
		}
	}
}

func (cache *LogCache) Debug(format string, a ...any) {
	log := Log{
		lvl: LogLevelDebug,
		msg: logMsg(LogLevelDebug, format, a...),
	}
	cache.logs = append(cache.logs, log)
}

func (cache *LogCache) Info(format string, a ...any) {
	log := Log{
		lvl: LogLevelInfo,
		msg: logMsg(LogLevelInfo, format, a...),
	}
	cache.logs = append(cache.logs, log)
}

func (cache *LogCache) Warning(format string, a ...any) {
	log := Log{
		lvl: LogLevelWarning,
		msg: logMsg(LogLevelWarning, format, a...),
	}
	cache.logs = append(cache.logs, log)
}

func (cache *LogCache) Error(format string, a ...any) {
	log := Log{
		lvl: LogLevelError,
		msg: logMsg(LogLevelError, format, a...),
	}
	cache.logs = append(cache.logs, log)
}

func (handler *LogHandler) Print(format string, a ...any) {
	handler.Logger.Print(sprintf(format, a...))
}

func (handler *LogHandler) Debug(format string, a ...any) {
	handler.Logger.Print(logMsg(LogLevelDebug, format, a...))
}

func (handler *LogHandler) Info(format string, a ...any) {
	handler.Logger.Print(logMsg(LogLevelInfo, format, a...))
}

func (handler *LogHandler) Warning(format string, a ...any) {
	handler.Logger.Print(logMsg(LogLevelWarning, format, a...))
}

func (handler *LogHandler) Error(format string, a ...any) {
	handler.Logger.Print(logMsg(LogLevelError, format, a...))
}

func logMsg(lvl arborist.LogLevel, format string, a ...any) string {
	msg := sprintf(format, a...)
	msg = fmt.Sprintf("%s: %s", lvl, msg)
	// get the call from 2 stack frames above this
	// (one call up is the LogCache method, so go one more above that)
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
	var msg string
	if len(a) == 0 {
		msg = format
	} else {
		msg = fmt.Sprintf(format, a...)
	}
	return msg
}
