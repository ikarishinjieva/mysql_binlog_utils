package mysql_binlog_utils

import (
	"io/ioutil"
	"path/filepath"
	"strings"
)

func GetFirstPreviousGtidOfBinlogDir(binlogDir, binlogBaseName string) (gtidDesc string, err error) {
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

	firstFile := filepath.Join(binlogDir, binlogFiles[0])
	ret, err := GetPreviousGtids(firstFile)
	return ret, err
}
