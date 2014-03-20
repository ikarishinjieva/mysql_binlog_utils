package main

import (
	util ".."
	"flag"
	"fmt"
	"os"
	"strconv"
)

func main() {
	flag.Parse()
	file := flag.Arg(1)
	if size, err := strconv.Atoi(flag.Arg(2)); nil != err {
		fmt.Println("endPos is not a integer")
		os.Exit(1)
	} else if err := util.RotateRelayLog(file, size); nil != err {
		fmt.Printf("err: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("ok")
}
