package mysql_binlog_utils

import (
	"fmt"
	"os"
	"os/exec"
	"testing"
)

func TestRotateRelayLog(t *testing.T) {
	testFile := "./test/mysql-relay-bin.000008"
	exec.Command("sh", "-c", "cp ./test/mysql-relay-bin.unfinished "+testFile).Run()
	defer os.Remove(testFile)
	stat, _ := os.Stat(testFile)
	err := RotateRelayLog(testFile, int(stat.Size()))
	if nil != err {
		t.Errorf("got err %v", err)
	}

	if err := exec.Command("sh", "-c", fmt.Sprintf("./test/mysqlbinlog %v > /dev/null", testFile)).Run(); nil != err {
		t.Errorf("mysqlbinlog not pass, err=%v", err)
	}
}
