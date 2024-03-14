package helpers

type Matrix [][]byte

type MatrixHeap []Matrix

func (h MatrixHeap) Len() int           { return len(h) }
func (h MatrixHeap) Less(i, j int) bool { return CompareMatrix(h[i], h[j]) == -1 }
func (h MatrixHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *MatrixHeap) Push(x any) {
	*h = append(*h, x.(Matrix))
}

func (h *MatrixHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0:n-1]
	return x
}

func (h MatrixHeap) Get(i int) Matrix {
	return h[i]
}

func (h MatrixHeap) First() Matrix {
	return h[0]
}

func (h MatrixHeap) Last() Matrix {
	return h[len(h)-1]
}
