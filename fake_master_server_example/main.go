package main

import (
	util ".."
	"fmt"
)

type StdLogger struct{}

func (s *StdLogger) Tracef(f string, args ...interface{}) {
	fmt.Printf(f+"\n", args...)
}

func main() {
	util.SetLogger(&StdLogger{})
	server := util.NewFakeMasterServer(3306, 999, 33, false, "/Users/Tac/Code/mysql_vm")
	err := server.Start()
	fmt.Printf("err=%v\n", err)
}
