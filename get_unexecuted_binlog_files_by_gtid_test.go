package mysql_binlog_utils

import (
	"testing"
)

func TestGetUnexecutedBinlogFilesByGtid(t *testing.T) {
	files, err := GetUnexecutedBinlogFilesByGtid("./test", "mysql-bin56",
		"7e23401a-c603-11e3-8e13-5e10e6a05cfb:1-6,8186fc1e-c5ff-11e3-8df9-e66ccf50db66:1-11,a6ce328c-c602-11e3-8e0d-e66ccf50db66:1-6,b7009920-c601-11e3-8e07-5e10e6a05cfb:1-6,f60ab33c-c604-11e3-8e1c-e66ccf50db66:1-3",
		false)
	if nil != err {
		t.Fatalf("unexpected err, %v", err)
	}
	if 1 != len(files) || "mysql-bin56.000003" != files[0] {
		t.Fatalf("wrong result: %v", files)
	}
}

func TestGetUnexecutedBinlogFilesByGtid2(t *testing.T) {
	files, err := GetUnexecutedBinlogFilesByGtid("./test", "mysql-bin56",
		"7e23401a-c603-11e3-8e13-5e10e6a05cfb:1-5,8186fc1e-c5ff-11e3-8df9-e66ccf50db66:1-11,a6ce328c-c602-11e3-8e0d-e66ccf50db66:1-6,b7009920-c601-11e3-8e07-5e10e6a05cfb:1-6,f60ab33c-c604-11e3-8e1c-e66ccf50db66:1-3",
		false)
	if nil != err {
		t.Fatalf("unexpected err, %v", err)
	}
	if 2 != len(files) || "mysql-bin56.000002" != files[0] || "mysql-bin56.000003" != files[1] {
		t.Fatalf("wrong result: %v", files)
	}
}
