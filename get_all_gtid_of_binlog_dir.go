package mysql_binlog_utils

import (
	gtid "github.com/ikarishinjieva/go-gtid"
	"io/ioutil"
	"path/filepath"
	"strings"
)

func GetAllGtidOfBinlogDir(binlogDir, binlogBaseName string) (gtidDesc string, err error) {
	files, err := ioutil.ReadDir(binlogDir)
	if nil != err {
		return "", err
	}

	var binlogFiles []string
	for _, file := range files {
		if strings.HasPrefix(file.Name(), binlogBaseName+".") && binlogFileSuffixPattern.MatchString(file.Name()) {
			binlogFiles = append(binlogFiles, file.Name())
		}
	}

	if 0 == len(binlogFiles) {
		return "", nil
	}

	lastFile := filepath.Join(binlogDir, binlogFiles[len(binlogFiles)-1])
	lastPreviousGtid, err := GetPreviousGtids(lastFile)
	if nil != err {
		return "", err
	}
	lastBinlogGtid, err := GetGtidOfBinlog(lastFile)
	if nil != err {
		return "", err
	}
	sum, err := gtid.GtidAdd(lastPreviousGtid, lastBinlogGtid)
	if nil != err {
		return "", err
	}
	return sum, nil
}
