package mysql_binlog_util

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"
)

func TestNewBinlogFileParser(t *testing.T) {
	ioutil.WriteFile("./test/test-mysql-bin-break-magic-number", []byte("1"), 0755)
	defer os.Remove("./test/test-mysql-bin-break-magic-number")

	if p, err := NewBinlogFileParserByPath("./test/test-mysql-bin-break-magic-number"); nil == err {
		p.Destroy()
		t.Errorf("expect err, but failed")
	}

	if p, err := NewBinlogFileParserByPath("./test/test-mysql-bin"); nil != err {
		t.Errorf("expect no err, bug got %v", err)
	} else {
		p.Destroy()
	}
}

func TestReadEventFixedHeader(t *testing.T) {
	p, _ := NewBinlogFileParserByPath("./test/test-mysql-bin")
	defer p.Destroy()
	if e, err := p.ReadEventFixedHeader(4); nil != err {
		t.Errorf("expect no err, but got %v", err)
	} else if e.EventType != FORMAT_DESCRIPTION_EVENT {
		t.Errorf("expect FDE but got %v", e.EventType)
	} else if e.EventLength != 103 {
		t.Errorf("expect eventLength=103 but got %v", e.EventLength)
	} else if e.NextPosition != 107 {
		t.Errorf("expect nextPosition=107 but got %v", e.NextPosition)
	}
}

type StdLogger struct{}

func (s *StdLogger) Tracef(f string, args ...interface{}) {
	fmt.Printf(f, args...)
}

func TestReadEventFixedHeader2(t *testing.T) {
	SetLogger(&StdLogger{})
	p, _ := NewBinlogFileParserByPath("./test/test-mysql-bin")
	defer p.Destroy()
	if e, err := p.ReadEventFixedHeader(562); nil != err {
	} else {
		fmt.Printf("%+v", e)
	}
}
