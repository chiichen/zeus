package main

import (
	"fmt"

	"github.com/chiichen/zeus/svr/packages/greet"
)

func main() {
	fmt.Println(greet.Hello("zeus"))
}
