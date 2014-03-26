package main

import (
	util ".."
	"fmt"
)

func main() {

	//parse binlog example
	p, err := util.NewBinlogFileParserByPath("/Users/Tac/Code/mysql_vm/mysql-bin.000013")
	if nil != err {
		fmt.Printf("err=%v\n", err)
	}
	defer p.Destroy()
	pos := 4
	for {
		fmt.Printf("pos=%v\n", pos)
		if e, err := p.ReadEventFixedHeader(pos); nil != err {
			fmt.Printf("err=%v\n", err)
			return
		} else {
			fmt.Printf("event=%+v\n", e)
			pos = pos + e.EventLength //e.NextPosition is not reliable
			if 0 == pos {
				fmt.Printf("done\n")
				return
			}
		}
	}
}
