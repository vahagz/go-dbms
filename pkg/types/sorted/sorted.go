package sorted

import "go-dbms/pkg/types"

type HeapItem[T any] struct {
	Key types.DataRow
	Val T
}

type Heap[T any] struct {
	Keys []string
	list []HeapItem[T]
}

func (h Heap[T]) Less(i, j int) bool {
	for _, col := range h.Keys {
		if h.list[i].Key[col].Compare("<", h.list[j].Key[col]) {
			return true
		}
	}
	return false
}
func (h Heap[T]) Len() int           { return len(h.list) }
func (h Heap[T]) Swap(i, j int)      { h.list[i], h.list[j] = h.list[j], h.list[i] }

func (h *Heap[T]) Push(x any) {
	h.list = append(h.list, x.(HeapItem[T]))
}

func (h *Heap[T]) Pop() any {
	old := h.list
	n := len(old)
	x := old[n-1]
	h.list = old[0:n-1]
	return x
}

func (h Heap[T]) Get(i int) HeapItem[T] {
	return h.list[i]
}

func (h Heap[T]) First() HeapItem[T] {
	return h.list[0]
}

func (h Heap[T]) Last() HeapItem[T] {
	return h.list[len(h.list)-1]
}
