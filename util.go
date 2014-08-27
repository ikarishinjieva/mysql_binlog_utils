package mysql_binlog_utils

import (
	"encoding/hex"
	"strings"
)

func intToBytes(num int, buf []byte) []byte {
	for i := 0; i < len(buf); i++ {
		buf[i] = byte(num & 0xff)
		num = num >> 8
	}
	return buf
}
func uintToBytes(num uint, buf []byte) []byte {
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

func bytesToUint(buf []byte) uint {
	var a uint
	var i uint
	for _, b := range buf {
		a += uint(b) << i
		i += 8
	}
	return a
}

func bytesToUint64(buf []byte) uint64 {
	var a uint64
	var i uint
	for _, b := range buf {
		a += uint64(b) << i
		i += 8
	}
	return a
}

func bytesToUuid(buf []byte) (ret string) {
	return strings.ToUpper(hex.EncodeToString(buf))
}
