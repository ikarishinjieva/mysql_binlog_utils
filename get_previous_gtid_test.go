package mysql_binlog_utils

import (
	"testing"
)

func TestGetPreviousGtids(t *testing.T) {
	gtid, err := GetPreviousGtids("./test/mysql-bin56.000002")
	if nil != err {
		t.Fatalf("unexpected error: %v", err)
	}
	if "7E23401AC60311E38E135E10E6A05CFB:1-5,8186FC1EC5FF11E38DF9E66CCF50DB66:1-11,A6CE328CC60211E38E0DE66CCF50DB66:1-6,B7009920C60111E38E075E10E6A05CFB:1-6" != gtid {
		t.Fatalf("wrong gtid %v", gtid)
	}
}
