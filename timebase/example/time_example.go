package main

import (
	"fmt"

	"timebase"
)

func main() {
	st := timebase.NowTimeFormat()

	//aUTC := timebase.Parse(st)
	//sCST := timebase.ParseInLocation(st)

	fmt.Println(timebase.Parse(st))
	fmt.Println(timebase.ParseInLocation(st))

}
