package mysql_binlog_utils

import (
	"testing"
)

func TestGetFirstPreviousGtidOfBinlogDir(t *testing.T) {
	gtidDesc, err := GetFirstPreviousGtidOfBinlogDir("./test", "mysql-bin56")
	if nil != err {
		t.Fatalf("unexpected error %v", err)
	}
	if "" != gtidDesc {
		t.Fatalf("wrong gtid %v", gtidDesc)
	}
}
