package main

import (
	"fmt"
	"math"
)

func main() {
	fmt.Println(math.Float64frombits(math.Float64bits(0.1+0.2)))
}