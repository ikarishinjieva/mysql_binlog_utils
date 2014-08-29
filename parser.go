package mysql_binlog_utils

import (
	"bufio"
	"fmt"
	"os"
)

type BinlogFileParser struct {
	filename       string
	file           *os.File
	needCloseFile  bool
	fileSize       uint
	bufReader      *bufio.Reader
	lastBufReadPos uint
}

func NewBinlogFileParser(file *os.File) (*BinlogFileParser, error) {
	ret := BinlogFileParser{}
	if a, err := file.Stat(); nil != err {
		return nil, err
	} else {
		_, err := file.Seek(0, 0)
		if nil != err {
			return nil, err
		}
		ret.file = file
		ret.filename = file.Name()
		ret.fileSize = uint(a.Size()) //binlog file size is no more than an uint
		ret.bufReader = bufio.NewReaderSize(file, 10*1024)
		ret.lastBufReadPos = 0
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

func (b *BinlogFileParser) readBytes(startPos uint, count uint) ([]byte, error) {
	buf := make([]byte, count)
	// fmt.Printf("startPos=%v, count=%v, lastBufReadPos=%v\n", startPos, count, b.lastBufReadPos)
	force := false
	if startPos != b.lastBufReadPos || force {
		_, err := b.file.Seek(int64(startPos), 0)
		if nil != err {
			return nil, err
		}
		b.bufReader.Reset(b.file)
	}

	readCount := uint(0)

	for readCount < count {
		c, err := b.bufReader.Read(buf[readCount:])
		if nil != err {
			return nil, err
		}
		readCount += uint(c)
	}
	b.lastBufReadPos = startPos + count
	// fmt.Printf("buf=%v\n", buf)
	tracef("read binlog file %v (startPos=%v), count=%v", b.filename, startPos, count)
	return buf, nil
}

func (b *BinlogFileParser) readUint(startPos uint, count uint) (uint, error) {
	if buf, err := b.readBytes(startPos, count); nil != err {
		return 0, err
	} else {
		return b.toUint(buf), nil
	}
}

func (b *BinlogFileParser) toUint(buf []byte) uint {
	var a uint
	var i uint
	for _, b := range buf {
		a += uint(b) << i
		i += 8
	}
	return a
}

func (b *BinlogFileParser) VerifyMagicNumber() error {
	if buf, err := b.readBytes(0, 4); nil != err {
		return err
	} else if buf[0] != 0xfe || buf[1] != 0x62 || buf[2] != 0x69 || buf[3] != 0x6e {
		return fmt.Errorf("read binlog file %v failed, magic number is [%X %X %X %X] (expect to [0xfe 0x62 0x69 0x6e])", b.filename, buf[0], buf[1], buf[2], buf[3])
	}
	return nil
}

func (b *BinlogFileParser) FileSize() uint {
	return b.fileSize
}

func (b *BinlogFileParser) ReadEventFixedHeader(startPos uint) (EventFixedHeader, error) {
	ret := EventFixedHeader{}

	buf, err := b.readBytes(startPos, 19)
	if nil != err {
		return ret, err
	}
	ret.Bytes = buf
	ret.Timestamp = int(b.toUint(buf[0:4]))
	ret.EventType = int(buf[4])
	ret.ServerId = int(b.toUint(buf[5:9]))
	ret.EventLength = b.toUint(buf[9:13])
	if ret.EventLength > MAX_ALLOWED_PACKET {
		return ret, fmt.Errorf("event length (%v) > MAX_ALLOWED_PACKET", ret.EventLength)
	}
	if ret.EventLength < LOG_EVENT_FIXED_HEADER_LEN {
		return ret, fmt.Errorf("event length (%v) < LOG_EVENT_FIXED_HEADER_LEN", ret.EventLength)
	}
	ret.NextPosition = int(b.toUint(buf[13:17]))
	ret.Flags = int(b.toUint(buf[17:19]))

	return ret, nil
}

func (b *BinlogFileParser) ReadEventBytes(startPos uint) (EventFixedHeader, []byte, error) {
	header, err := b.ReadEventFixedHeader(startPos)
	if nil != err {
		return header, nil, err
	}
	remainBytes, err := b.readBytes(startPos+19, header.EventLength-19)
	if nil != err {
		return header, nil, err
	}
	return header, append(header.Bytes, remainBytes...), nil
}
