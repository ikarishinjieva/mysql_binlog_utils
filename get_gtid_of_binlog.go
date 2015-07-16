package mysql_binlog_utils

import (
	"encoding/binary"
	gtid "github.com/ikarishinjieva/go-gtid"
	"io"
	"os"
	"strconv"
)

func GetGtidOfBinlog(binlogPath string) (gtidDesc string, err error) {
	file, err := os.Open(binlogPath)
	if nil != err {
		return "", err
	}
	defer file.Close()

	p := uint32(4)
	headerBs := make([]byte, 19)
	gtidBs := make([]byte, 25)

	for {
		if _, err := file.Seek(int64(p), 0); nil != err {
			if "EOF" == err.Error() {
				break
			}
			return gtidDesc, err
		}

		if _, err := io.ReadFull(file, headerBs); nil != err {
			if "EOF" == err.Error() {
				break
			}
			return gtidDesc, err
		}

		length := binary.LittleEndian.Uint32(headerBs[9:13])
		eventType := int(headerBs[4])

		if GTID_LOG_EVENT != eventType {
			p += length
			continue
		}

		if _, err := io.ReadFull(file, gtidBs); nil != err {
			if "EOF" == err.Error() {
				break
			}
			return gtidDesc, err
		}

		uuid := bytesToUuid(gtidBs[1:17])
		number := bytesToUint64(gtidBs[17:])
		if gtidDesc, err = gtid.GtidAdd(gtidDesc, uuid+":"+strconv.FormatUint(number, 10)); nil != err {
			return gtidDesc, err
		}

		p += length
	}
	return gtidDesc, nil
}
