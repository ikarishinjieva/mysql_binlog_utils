package mysql_binlog_utils

import (
	"fmt"
)

func GetUnexecutedBinlogPosByGtid(binlogFilePath string, executedGtidDesc string) (pos uint, err error) {
	parser, err := NewBinlogFileParserByPath(binlogFilePath)
	if nil != err {
		return 0, err
	}
	defer parser.Destroy()

	executedGtid, err := parseGtid(executedGtidDesc)
	if nil != err {
		return 0, err
	}

	p := uint(4)
	for {
		header, bs, err := parser.ReadEventBytes(p)
		if nil != err {
			return 0, err
		}
		if GTID_LOG_EVENT != header.EventType {
			p += header.EventLength
			continue
		}

		pos := 19
		uuid := bytesToUuid(bs[pos+1 : pos+17])
		number := bytesToUint64(bs[pos+17 : pos+17+8])
		gtid, err := parseGtid(fmt.Sprintf("%v:%v", uuid, number))
		if nil != err {
			return 0, err
		}
		if !containsGtid(executedGtid, gtid) {
			return p, nil
		}
		p += header.EventLength
	}
}
