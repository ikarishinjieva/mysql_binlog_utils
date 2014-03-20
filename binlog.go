package mysql_binlog_util

import (
	"fmt"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

func NextBinlogPath(binlogPath string) (string, error) {
	r := regexp.MustCompile("(.*)\\.(\\d\\d\\d\\d\\d\\d)$")
	if !r.MatchString(binlogPath) {
		return "", fmt.Errorf("path %v is not a binlog path", binlogPath)
	}
	matches := r.FindStringSubmatch(binlogPath)
	seq, _ := strconv.Atoi(matches[2])
	return fmt.Sprintf("%v.%06d", matches[1], seq+1), nil
}

func BinlogIndexPath(binlogPath string) (string, error) {
	r := regexp.MustCompile("(.*)\\.(\\d\\d\\d\\d\\d\\d)$")
	if !r.MatchString(binlogPath) {
		return "", fmt.Errorf("path %v is not a binlog path", binlogPath)
	}
	matches := r.FindStringSubmatch(binlogPath)
	return fmt.Sprintf("%v.index", matches[1]), nil
}

func NextBinlogName(binlogPath string) (string, error) {
	if path, err := NextBinlogPath(binlogPath); nil != err {
		return "", err
	} else {
		path = strings.Replace(path, "\\", "/", -1) //fix windows-style path
		return filepath.Base(path), nil
	}
}
