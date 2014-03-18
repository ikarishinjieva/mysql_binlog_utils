package mysql_binlog_util

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
)

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

	if a, err := b.readInt(startPos+0, 4); nil != err {
		return ret, err
	} else {
		ret.Timestamp = int(a)
	}

	if buf, err := b.readBytes(startPos+4, 1); nil != err {
		return ret, err
	} else {
		ret.EventType = int(buf[0])
	}

	if a, err := b.readInt(startPos+5, 4); nil != err {
		return ret, err
	} else {
		ret.ServerId = int(a)
	}

	if a, err := b.readInt(startPos+9, 4); nil != err {
		return ret, err
	} else {
		ret.EventLength = int(a)
		if ret.EventLength > MAX_ALLOWED_PACKET {
			return ret, fmt.Errorf("event length (%v) > MAX_ALLOWED_PACKET", ret.EventLength)
		}
		if ret.EventLength < LOG_EVENT_FIXED_HEADER_LEN {
			return ret, fmt.Errorf("event length (%v) < LOG_EVENT_FIXED_HEADER_LEN", ret.EventLength)
		}
	}

	if a, err := b.readInt(startPos+13, 4); nil != err {
		return ret, err
	} else {
		ret.NextPosition = int(a)
	}

	if a, err := b.readInt(startPos+17, 2); nil != err {
		return ret, err
	} else {
		ret.Flags = int(a)
	}

	return ret, nil
}
