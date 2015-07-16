package mysql_binlog_utils

import (
	"fmt"
	gtid "github.com/ikarishinjieva/go-gtid"
	"io/ioutil"
	"path/filepath"
	"strings"
)

func GetUnexecutedBinlogFilesByGtid(binlogDir string, binlogBaseName string, executedGtidDesc string, includeEventBeforeFirst bool) (
	ret []string, err error) {
	files, err := ioutil.ReadDir(binlogDir)
	if nil != err {
		return nil, err
	}

	var binlogFiles []string
	for _, file := range files {
		if strings.HasPrefix(file.Name(), binlogBaseName+".") && binlogFileSuffixPattern.MatchString(file.Name()) {
			binlogFiles = append(binlogFiles, file.Name())
		}
	}

	if 0 == len(binlogFiles) {
		return make([]string, 0), nil
	}

	for i := len(binlogFiles) - 1; i >= 0; i-- {
		binlogFile := binlogFiles[i]
		previousGtids, err := GetPreviousGtids(filepath.Join(binlogDir, binlogFile))
		if nil != err {
			return nil, err
		}
		contain, err := gtid.GtidContain(executedGtidDesc, previousGtids)
		if nil != err {
			return nil, err
		}
		eql, err := gtid.GtidEqual(executedGtidDesc, previousGtids)
		if nil != err {
			return nil, err
		}
		if contain && !(includeEventBeforeFirst && eql && "" != executedGtidDesc) {
			for j := i; j < len(binlogFiles); j++ {
				ret = append(ret, binlogFiles[j])
			}
			return ret, nil
		}
	}
	return nil, fmt.Errorf("Found unexecuted gtid comparing with previousGtids of even first binlog")
}
