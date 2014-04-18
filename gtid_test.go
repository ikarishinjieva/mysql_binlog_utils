package mysql_binlog_utils

import (
	"testing"
)

func TestparseGtid(t *testing.T) {
	gtid, err := parseGtid("ca8035ea-c5d5-11e3-8ce9-e66ccf50db66:1-11:13, ff92c4da-c5d7-11e3-8cf7-5e10e6a05cfb:1-5")
	if nil != err {
		t.Fatalf("unexpected error %v", err)
	}
	if 2 == len(gtid.sids) &&
		"CA8035EAC5D511E38CE9E66CCF50DB66" == gtid.sids[0].serverUuid &&
		2 == len(gtid.sids[0].intervals) &&
		1 == gtid.sids[0].intervals[0].from &&
		11 == gtid.sids[0].intervals[0].to &&
		13 == gtid.sids[0].intervals[1].from &&
		13 == gtid.sids[0].intervals[1].to &&
		"FF92C4DAC5D711E38CF75E10E6A05CFB" == gtid.sids[1].serverUuid &&
		1 == len(gtid.sids[1].intervals) &&
		1 == gtid.sids[1].intervals[0].from &&
		5 == gtid.sids[1].intervals[0].to {
		return
	}

	t.Fatalf("wrong gtid %+v", gtid)
}

func TestContainsGtid(t *testing.T) {
	a, _ := parseGtid("ca8035ea-c5d5-11e3-8ce9-e66ccf50db66:1-11:13-14")
	b, _ := parseGtid("ca8035ea-c5d5-11e3-8ce9-e66ccf50db66:2-11:13")
	c, _ := parseGtid("ca8035ea-c5d5-11e3-8ce9-e66ccf50db66:2-12")
	d, _ := parseGtid("ff92c4da-c5d7-11e3-8cf7-5e10e6a05cfb:1-11:13-14")
	if !containsGtid(a, b) {
		t.Fatal("wrong : a <=> b")
	}
	if containsGtid(a, c) {
		t.Fatal("wrong : a <=> c")
	}
	if containsGtid(a, d) {
		t.Fatal("wrong : a <=> d")
	}
}

func TestGetPreviousGtids(t *testing.T) {
	gtid, err := getPreviousGtids("./test/mysql-bin56.000002")
	if nil != err {
		t.Fatalf("unexpected error: %v", err)
	}
	if "7E23401AC6311E38E135E10E6A05CFB:1-5,8186FC1EC5FF11E38DF9E66CCF50DB66:1-11,A6CE328CC6211E38EDE66CCF50DB66:1-6,B709920C6111E38E75E10E6A05CFB:1-6" != gtid.String() {
		t.Fatalf("wrong gtid %v", gtid.String())
	}
}
