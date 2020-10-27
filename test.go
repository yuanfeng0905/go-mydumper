package main

import (
	"fmt"
	"strings"
)

func main() {
	a := "hello"

	fmt.Printf("%v, 10 %% 1=%d", strings.Split(a, ","), 0%8)
}
