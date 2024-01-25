package helpers

import "cmp"

func Min[T cmp.Ordered](numbers ...T) T {
	var min T = numbers[0]
	for _, n := range numbers {
		if n < min {
			min = n
		}
	}
	return min
}

func GetBit(BYTE, index uint8) bool {
	return (BYTE & (1<<index))>>index == 1
}

func SetBit(BYTE *uint8, index uint8, val bool) {
	if val {
		*BYTE = *BYTE | (1 << index)
	} else {
		*BYTE = *BYTE &^ (1 << index)
	}
}
