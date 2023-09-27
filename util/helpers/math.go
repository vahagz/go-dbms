package helpers

import "golang.org/x/exp/constraints"

func Min[T constraints.Ordered](numbers ...T) T {
	var min T = numbers[0]
	for _, n := range numbers {
		if n < min {
			min = n
		}
	}
	return min
}
