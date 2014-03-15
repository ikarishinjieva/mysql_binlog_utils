package mysql_binlog_util

type Logger interface {
	Tracef(fmt string, args ...interface{})
}

var l Logger

func tracef(fmt string, args ...interface{}) {
	if nil != l {
		l.Tracef(fmt, args...)
	}
}
