package mysql_binlog_utils

import (
	"encoding/binary"
	"io"
	"os"
	"strconv"
)

func GetPreviousGtids(binlogPath string) (gtidDesc string, err error) {
	file, err := os.Open(binlogPath)
	if nil != err {
		return "", err
	}
	defer file.Close()

	p := uint32(4)
	headerBs := make([]byte, 19)
	payloadBs := make([]byte, 1024)

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

		if PREVIOUS_GTIDS_LOG_EVENT != eventType {
			p += length
			continue
		}

		payloadLength := length - 19

		if payloadLength > uint32(len(payloadBs)) {
			payloadBs = make([]byte, payloadLength)
		}

		if _, err := io.ReadFull(file, payloadBs[:payloadLength]); nil != err {
			if "EOF" == err.Error() {
				break
			}
			return gtidDesc, err
		}

		ret := ""
		sidNumberCount := bytesToUint(payloadBs[0:8])
		pos := 8
		for i := uint(0); i < sidNumberCount; i++ {
			if "" != ret {
				ret = ret + ","
			}
			uuid := bytesToUuid(payloadBs[pos : pos+16])
			ret = ret + uuid
			internalCount := bytesToUint(payloadBs[pos+16 : pos+16+8])
			pos = pos + 16 + 8
			for i := uint(0); i < internalCount; i++ {
				from := bytesToUint64(payloadBs[pos : pos+8])
				to := bytesToUint64(payloadBs[pos+8:pos+16]) - 1
				pos = pos + 16
				ret = ret + ":" + strconv.FormatUint(from, 10) + "-" + strconv.FormatUint(to, 10)
			}
		}
		return ret, nil
	}
	return gtidDesc, nil
}
