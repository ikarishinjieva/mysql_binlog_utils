package mysql_binlog_utils

import (
	"fmt"
)

func GetUnexecutedBinlogPosByGtid(binlogFilePath string, executedGtidDesc string) (pos uint, err error) {
	parser, err := NewBinlogFileParserByPath(binlogFilePath)
	if nil != err {
		return 0, err
	}
	defer parser.Destroy()

	executedGtid, err := parseGtid(executedGtidDesc)
	if nil != err {
		return 0, err
	}

	poison := make(chan bool, 2)
	type tProcessQueueElement struct {
		bs  []byte
		pos uint
		err error
	}
	processQueue := make(chan *tProcessQueueElement, 100)

	//read thread
	go func() {
		p := uint(4)
		for {
			select {
			case <-poison:
				return
			default:
			}
			header, bs, err := parser.ReadEventBytes(p)
			if nil != err {
				select {
				case <-poison:
					return
				case processQueue <- &tProcessQueueElement{nil, 0, err}:
				}
				return
			}
			if GTID_LOG_EVENT != header.EventType {
				p += header.EventLength
				continue
			}
			select {
			case <-poison:
				return
			case processQueue <- &tProcessQueueElement{bs, p, nil}:
			}

			p += header.EventLength
		}
	}()

	//process thread

	type tReturnQueueElement struct {
		pos uint
		err error
	}
	returnQueue := make(chan *tReturnQueueElement, 1)

	go func() {
		for {
			var ele *tProcessQueueElement
			select {
			case <-poison:
				return
			case ele = <-processQueue:
			}
			if nil != ele.err {
				select {
				case <-poison:
					return
				case returnQueue <- &tReturnQueueElement{0, ele.err}:
				}
				return
			}
			bs := ele.bs
			uuid := bytesToUuid(bs[19+1 : 19+17])
			number := bytesToUint64(bs[19+17 : 19+17+8])
			gtid, err := parseGtid(fmt.Sprintf("%v:%v", uuid, number))
			if nil != err {
				select {
				case <-poison:
					return
				case returnQueue <- &tReturnQueueElement{0, err}:
				}
				return
			}
			if !containsGtid(executedGtid, gtid) {
				select {
				case <-poison:
					return
				case returnQueue <- &tReturnQueueElement{ele.pos, nil}:
				}
				return
			}
		}
	}()

	defer func() {
		poison <- true
		poison <- true
	}()

	select {
	case ret := <-returnQueue:
		return ret.pos, ret.err
	}
}
