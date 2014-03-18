package parse_binlog_example

import (
	util ".."
	"fmt"
)

func main() {

	//parse binlog example
	p, err := util.NewBinlogFileParserByPath("/Users/Tac/Code/actionsky/actionsky-ha/actionsky-ha-jruby/vm/x86_64/mysql-relay-bin.000004")
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
			pos = e.NextPosition
			if 0 == pos {
				fmt.Printf("done\n")
				return
			}
		}
	}
}
