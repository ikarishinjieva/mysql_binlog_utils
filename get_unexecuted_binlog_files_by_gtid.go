package mysql_binlog_utils

import (
	"fmt"
	"io/ioutil"
	"path/filepath"
	"strings"
)

func GetUnexecutedBinlogFilesByGtid(binlogDir string, binlogBaseName string, executedGtidDesc string) (ret []string, err error) {
	files, err := ioutil.ReadDir(binlogDir)
	if nil != err {
		return nil, err
	}

	var binlogFiles []string
	for _, file := range files {
		if strings.HasPrefix(file.Name(), binlogBaseName) {
			binlogFiles = append(binlogFiles, file.Name())
		}
	}

	if 0 == len(binlogFiles) {
		return nil, fmt.Errorf("no binlog file found in %v", binlogDir)
	}

	executedGtids, err := parseGtid(executedGtidDesc)
	if nil != err {
		return nil, err
	}

	for i := len(binlogFiles) - 1; i >= 0; i-- {
		binlogFile := binlogFiles[i]
		previousGtids, err := getPreviousGtids(filepath.Join(binlogDir, binlogFile))
		if nil != err {
			return nil, err
		}
		if containsGtid(executedGtids, previousGtids) {
			for j := i; j < len(binlogFiles); j++ {
				ret = append(ret, binlogFiles[j])
			}
			return ret, nil
		}
	}
	return nil, fmt.Errorf("Found unexecuted gtid comparing with previousGtids of even first binlog")
}
