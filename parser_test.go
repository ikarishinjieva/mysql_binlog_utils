package mysql_binlog_util

import (
	"io/ioutil"
	"os"
	"testing"
)

func TestNewBinlogFileParser(t *testing.T) {
	ioutil.WriteFile("test-mysql-bin-break-magic-number", []byte("1"), 0755)
	defer os.Remove("test-mysql-bin-break-magic-number")

	if p, err := NewBinlogFileParserByPath("test-mysql-bin-break-magic-number"); nil == err {
		p.Destroy()
		t.Errorf("expect err, but failed")
	}

	if p, err := NewBinlogFileParserByPath("test-mysql-bin"); nil != err {
		t.Errorf("expect no err, bug got %v", err)
	} else {
		p.Destroy()
	}
}

func TestReadEventFixedHeader(t *testing.T) {
	p, _ := NewBinlogFileParserByPath("test-mysql-bin")
	defer p.Destroy()
	if e, err := p.ReadEventFixedHeader(4); nil != err {
		t.Errorf("expect no err, but got %v", err)
	} else if e.eventType != FORMAT_DESCRIPTION_EVENT {
		t.Errorf("expect FDE but got %v", e.eventType)
	} else if e.eventLength != 103 {
		t.Errorf("expect eventLength=103 but got %v", e.eventLength)
	} else if e.nextPosition != 107 {
		t.Errorf("expect nextPosition=107 but got %v", e.nextPosition)
	}

}
