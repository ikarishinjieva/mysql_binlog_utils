package mysql_binlog_util

import (
	"fmt"
	"os"
)

func RotateRelayLog(relayLogPath string, endPos int) error {
	f, err := os.OpenFile(relayLogPath, os.O_RDWR, 0660)
	if nil != err {
		return err
	}
	defer f.Close()

	if info, err := f.Stat(); nil != err {
		return err
	} else if info.Size() < int64(endPos) {
		return fmt.Errorf("relay log size(%v) < end pos(%v)", info.Size(), endPos)
	}

	parser, err := NewBinlogFileParser(f)
	if nil != err {
		return err
	}
	defer parser.Destroy()

	firstEventFixedHeader, err := parser.ReadEventFixedHeader(4)
	if nil != err {
		return err
	}

	lastEventFixedHeader := EventFixedHeader{}
	lastEventFixedHeader.Timestamp = firstEventFixedHeader.Timestamp
	lastEventFixedHeader.EventType = ROTATE_EVENT
	lastEventFixedHeader.ServerId = firstEventFixedHeader.ServerId
	//lastEventFixedHeader.EventLength will be set in GenBinlogEventBytes
	lastEventFixedHeader.NextPosition = 0
	lastEventFixedHeader.Flags = 0x40 //LOG_EVENT_RELAY_LOG_F

	lastEventFixedData := EventFixedData{}
	lastEventFixedData.Bytes = []byte{0x4, 0, 0, 0, 0, 0, 0, 0}

	lastEventVariableData := EventVariableData{}
	if a, err := NextBinlogName(relayLogPath); nil != err {
		return err
	} else {
		lastEventVariableData.Bytes = []byte(a)
	}

	rotateBytes, err := GenBinlogEventBytes(lastEventFixedHeader, lastEventFixedData, lastEventVariableData)
	if nil != err {
		return err
	}
	if _, err := f.WriteAt(rotateBytes, int64(endPos)); nil != err {
		return err
	} else if err := f.Truncate(int64(endPos + len(rotateBytes))); nil != err {
		return err
	}
	return nil
}
