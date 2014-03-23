package mysql_binlog_util

import (
	"bytes"
	"encoding/binary"
)

func intToBytes(num int, buf []byte) []byte {
	for i := 0; i < len(buf); i++ {
		buf[i] = byte(num & 0xff)
		num = num >> 8
	}
	return buf
}

func stringNullToBytes(a string) []byte {
	ret := []byte(a)
	ret = append(ret, byte(0))
	return ret
}

func bytesToUint(a []byte) int {
	if a, err := binary.ReadUvarint(bytes.NewBuffer(a)); nil != err {
		panic(err)
	} else {
		return int(a)
	}
}
