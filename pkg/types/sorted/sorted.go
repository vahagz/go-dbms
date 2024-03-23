package sorted

import (
	"go-dbms/pkg/types"
)

type HeapItem[T interface{}] struct {
	Key types.DataRow
	Val T
}

type Heap[T interface{}] struct {
	Keys []string
	list []*HeapItem[T]
}

// heap.Interface implementation
func (h *Heap[T]) Len() int                { return len(h.list) }
func (h *Heap[T]) Less(i, j int) bool      { return h.list[i].Key.Compare(h.list[j].Key, h.Keys) < 0 }
func (h *Heap[T]) Swap(i, j int)           { h.list[i], h.list[j] = h.list[j], h.list[i] }
func (h *Heap[T]) Push(x interface{})      { h.list = append(h.list, x.(*HeapItem[T])) }
func (h *Heap[T]) Pop() (last interface{}) { last, h.list = h.list[len(h.list)-1], h.list[:len(h.list)-1]; return }
