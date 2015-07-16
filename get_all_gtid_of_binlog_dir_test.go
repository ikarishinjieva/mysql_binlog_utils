package mysql_binlog_utils

import (
	"testing"
)

func TestGetAllGtidOfBinlogDir(t *testing.T) {
	desc, err := GetAllGtidOfBinlogDir("./test", "mysql-bin56")
	if nil != err {
		t.Fatalf("unexpected error %v", err)
	}
	if "7E23401AC60311E38E135E10E6A05CFB:1-6,8186FC1EC5FF11E38DF9E66CCF50DB66:1-11,A6CE328CC60211E38E0DE66CCF50DB66:1-6,B7009920C60111E38E075E10E6A05CFB:1-6,F60AB33CC60411E38E1CE66CCF50DB66:1-136" != desc {
		t.Fatalf("wrong gtid %v", desc)
	}
}
