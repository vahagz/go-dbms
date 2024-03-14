package stream

func New[T any](size int) Stream[T] {
	return &stream[T]{
		ch:   make(chan T, size),
		next: make(chan bool, size),
	}
}

type Reader[T any] interface {
	Pop() (T, bool)
	Slice() []T
}

type ReaderContinue[T any] interface {
	Reader[T]
	Continue(bool)
}

type Writer[T any] interface {
	Push(T)
	Close()
}

type WriterContinue[T any] interface {
	Writer[T]
	ShouldContinue() bool
}

type Stream[T any] interface {
	ReaderContinue[T]
	WriterContinue[T]
}

type stream[T any] struct {
	ch   chan T
	next chan bool
}

func (s *stream[T]) Pop() (T, bool) {
	val, ok := <-s.ch
	return val, ok
}

func (s *stream[T]) Slice() []T {
	sl := []T{}
	for itm := range s.ch {
		sl = append(sl, itm)
	}
	return sl
}

func (s *stream[T]) Continue(v bool) {
	s.next<-v
}

func (s *stream[T]) Push(val T) {
	s.ch<-val
}

func (s *stream[T]) ShouldContinue() bool {
	return <-s.next
}

func (s *stream[T]) Close() {
	close(s.ch)
	close(s.next)
}
