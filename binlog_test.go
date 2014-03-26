package mysql_binlog_utils

import (
	"testing"
)

func TestNextBinlogPath(t *testing.T) {
	a, err := NextBinlogPath("/opt/mysql-relay-log.000199")
	if nil != err {
		t.Errorf("got err %v", err)
	}
	if "/opt/mysql-relay-log.000200" != a {
		t.Errorf("got wrong return %v", a)
	}
}

func TestNextBinlogName(t *testing.T) {
	a, err := NextBinlogName("/opt/mysql-relay-log.000199")
	if nil != err {
		t.Errorf("got err %v", err)
	}
	if "mysql-relay-log.000200" != a {
		t.Errorf("got wrong return %v", a)
	}
}
