package logging

import "github.com/uc-cdis/arborist/arborist"

type Cache struct {
	logs []Entry
}

type Entry struct {
	lvl arborist.LogLevel
	msg string
}

func (cache *Cache) Write(logger arborist.Logger) {
	l, ok := logger.(*Handler)
	if !ok {
		l = newHandler()
	}
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

func (cache *Cache) Debug(format string, a ...any) {
	cache.logs = append(cache.logs, Entry{lvl: LogLevelDebug, msg: logMsg(LogLevelDebug, format, a...)})
}

func (cache *Cache) Info(format string, a ...any) {
	cache.logs = append(cache.logs, Entry{lvl: LogLevelInfo, msg: logMsg(LogLevelInfo, format, a...)})
}

func (cache *Cache) Warning(format string, a ...any) {
	cache.logs = append(cache.logs, Entry{lvl: LogLevelWarning, msg: logMsg(LogLevelWarning, format, a...)})
}

func (cache *Cache) Error(format string, a ...any) {
	cache.logs = append(cache.logs, Entry{lvl: LogLevelError, msg: logMsg(LogLevelError, format, a...)})
}
