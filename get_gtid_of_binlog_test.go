package mysql_binlog_utils

import (
	"testing"
)

func TestGetGtidOfBinlog(t *testing.T) {
	desc, err := GetGtidOfBinlog("./test/mysql-bin56.000003")
	if nil != err {
		t.Fatalf("unexpected error %v", err)
	}
	if "F60AB33CC60411E38E1CE66CCF50DB66:1-136" != desc {
		t.Fatalf("wrong gtid %v", desc)
	}
}
