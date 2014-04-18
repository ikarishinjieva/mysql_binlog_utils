package mysql_binlog_utils

import (
	"testing"
)

func TestGetUnexecutedBinlogPosByGtid(t *testing.T) {
	executedGtid := "f60ab33c-c604-11e3-8e1c-e66ccf50db66:1-124"
	pos, err := GetUnexecutedBinlogPosByGtid("./test/mysql-bin56.000003", executedGtid)
	if nil != err {
		t.Fatalf("unexpected error: %v", err)
	}
	if 125553 != pos {
		t.Fatalf("wrong pos %v", pos)
	}
}

func TestGetUnexecutedBinlogPosByGtid2(t *testing.T) {
	executedGtid := "f60ab33c-c604-11e3-8e1c-e66ccf50db66:1-136"
	_, err := GetUnexecutedBinlogPosByGtid("./test/mysql-bin56.000003", executedGtid)
	if nil == err || "EOF" != err.Error() {
		t.Fatalf("wrong err %v", err)
	}
}
