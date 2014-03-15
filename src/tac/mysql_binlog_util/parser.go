package mysql_binlog_util

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
)

const (
	UNKNOWN_EVENT            = 0
	START_EVENT_V3           = 1
	QUERY_EVENT              = 2
	STOP_EVENT               = 3
	ROTATE_EVENT             = 4
	INTVAR_EVENT             = 5
	LOAD_EVENT               = 6
	SLAVE_EVENT              = 7
	CREATE_FILE_EVENT        = 8
	APPEND_BLOCK_EVENT       = 9
	EXEC_LOAD_EVENT          = 10
	DELETE_FILE_EVENT        = 11
	NEW_LOAD_EVENT           = 12
	RAND_EVENT               = 13
	USER_VAR_EVENT           = 14
	FORMAT_DESCRIPTION_EVENT = 15
	XID_EVENT                = 16
	BEGIN_LOAD_QUERY_EVENT   = 17
	EXECUTE_LOAD_QUERY_EVENT = 18
	TABLE_MAP_EVENT          = 19
	PRE_GA_WRITE_ROWS_EVENT  = 20
	PRE_GA_UPDATE_ROWS_EVENT = 21
	PRE_GA_DELETE_ROWS_EVENT = 22
	WRITE_ROWS_EVENT_V1      = 23
	UPDATE_ROWS_EVENT_V1     = 24
	DELETE_ROWS_EVENT_V1     = 25
	INCIDENT_EVENT           = 26
	HEARTBEAT_LOG_EVENT      = 27
	IGNORABLE_LOG_EVENT      = 28
	ROWS_QUERY_LOG_EVENT     = 29
	WRITE_ROWS_EVENT         = 30
	UPDATE_ROWS_EVENT        = 31
	DELETE_ROWS_EVENT        = 32
	GTID_LOG_EVENT           = 33
	ANONYMOUS_GTID_LOG_EVENT = 34
	PREVIOUS_GTIDS_LOG_EVENT = 35
)

const (
	LOG_EVENT_MINIMAL_HEADER_LEN = 19
	MAX_ALLOWED_PACKET           = 1024 * 1024 * 1024
)

type EventFixedHeader struct {
	eventType    int
	eventLength  int
	nextPosition int
}

type BinlogFileParser struct {
	filename      string
	file          *os.File
	needCloseFile bool
	fileSize      int
}

func NewBinlogFileParser(file *os.File) (*BinlogFileParser, error) {
	ret := BinlogFileParser{}
	if a, err := file.Stat(); nil != err {
		return nil, err
	} else {
		ret.file = file
		ret.filename = file.Name()
		ret.fileSize = int(a.Size()) //binlog file size is no more than an int
	}
	if err := ret.VerifyMagicNumber(); nil != err {
		return nil, err
	}
	return &ret, nil
}

func NewBinlogFileParserByPath(filepath string) (*BinlogFileParser, error) {
	ret := BinlogFileParser{}
	if file, err := os.Open(filepath); nil != err {
		return nil, err
	} else {
		ret.needCloseFile = true
		return NewBinlogFileParser(file)
	}
}

func (b *BinlogFileParser) Destroy() error {
	if b.needCloseFile && nil != b.file {
		b.file.Close()
	}
	return nil
}

func (b *BinlogFileParser) readBytes(startPos int, count int) ([]byte, error) {
	buf := make([]byte, count)
	if c, err := b.file.ReadAt(buf, int64(startPos)); count != c || nil != err {
		return nil, fmt.Errorf("read binlog file %v (startPos=%v) failed, err=%v, count=%v (expect to %v)", b.filename, startPos, err, c, count)
	} else {
		tracef("read binlog file %v (startPos=%v), count=%v, ret=%+v\n", b.filename, startPos, count, buf)
		return buf, nil
	}
}

func (b *BinlogFileParser) readInt(startPos int, count int) (int, error) {
	if buf, err := b.readBytes(startPos, count); nil != err {
		return 0, err
	} else if l, err := binary.ReadUvarint(bytes.NewBuffer(buf)); nil != err {
		return 0, err
	} else {
		return int(l), nil
	}
}

func (b *BinlogFileParser) VerifyMagicNumber() error {
	if buf, err := b.readBytes(0, 4); nil != err {
		return err
	} else if buf[0] != 0xfe || buf[1] != 0x62 || buf[2] != 0x69 || buf[3] != 0x6e {
		return fmt.Errorf("read binlog file %v failed, magic number is [%X %X %X %X] (expect to [0xfe 0x62 0x69 0x6e])", b.filename, buf[0], buf[1], buf[2], buf[3])
	}
	return nil
}

func (b *BinlogFileParser) FileSize() int {
	return b.fileSize
}

func (b *BinlogFileParser) ReadEventFixedHeader(startPos int) (EventFixedHeader, error) {
	ret := EventFixedHeader{}
	if buf, err := b.readBytes(startPos+4, 1); nil != err {
		return ret, err
	} else {
		ret.eventType = int(buf[0])
	}

	if a, err := b.readInt(startPos+9, 4); nil != err {
		return ret, err
	} else {
		ret.eventLength = int(a)
		if ret.eventLength > MAX_ALLOWED_PACKET {
			return ret, fmt.Errorf("event length (%v) > MAX_ALLOWED_PACKET", ret.eventLength)
		}
		if ret.eventLength < LOG_EVENT_MINIMAL_HEADER_LEN {
			return ret, fmt.Errorf("event length (%v) < LOG_EVENT_MINIMAL_HEADER_LEN", ret.eventLength)
		}
	}

	if a, err := b.readInt(startPos+13, 4); nil != err {
		return ret, err
	} else {
		ret.nextPosition = int(a)
	}

	return ret, nil
}
