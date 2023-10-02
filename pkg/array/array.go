package array

import (
	"encoding/binary"
	"errors"
	"fmt"
	"go-dbms/pkg/pager"
)

var bin = binary.BigEndian
var ErrOutOfBounds = errors.New("out of bounds")
var ErrEmpty = errors.New("empty")

func Open[T elementer[U], U any](fileName string, opts *ArrayOptions) (*Array[T, U], error) {
	p, err := pager.Open(fileName, int(opts.PageSize), false, 0664)
	if err != nil {
		return nil, err
	}

	fl := &Array[T, U]{
		pager: p,
		pages: map[uint64]*page[T, U]{},
	}

	return fl, fl.open(opts)
}

type ArrayADS[T elementer[U], U any] interface {
	Push(elem T) (uint64, error)
	PushMem(elem T) (uint64, error)
	Get(index uint64) (T, error)
	Set(index uint64, elem T) (error)
	SetMem(index uint64, elem T) (error)
	Size() uint64
	Truncate(size uint64) error
	Scan(index uint64, reverse bool, scanFn func(index uint64, elem T) (bool, error)) error
	WriteAll() error
	Close() error
	Print() error
}

type Array[T elementer[U], U any] struct {
	meta  *metadata
	pager *pager.Pager
	pages map[uint64]*page[T, U]
}

func (arr *Array[T, U]) Push(elem T) (uint64, error) {
	index, err := arr.PushMem(elem)
	if err != nil {
		return 0, err
	}
	return index, arr.writeAll()
}

func (arr *Array[T, U]) PushMem(elem T) (uint64, error) {
	p, err := arr.lastPage()
	if err != nil {
		return 0, err
	}

	p.dirty = true
	p.elems = append(p.elems, elem)
	arr.meta.dirty = true
	arr.meta.size++
	return uint64(len(p.elems) - 1), nil
}

func (arr *Array[T, U]) Get(index uint64) (T, error) {
	ptr := arr.pointer(index)
	if !arr.isValid(ptr) {
		return nil, ErrOutOfBounds
	}

	p, err := arr.fetch(ptr.pageId)
	if err != nil {
		return nil, err
	}

	return p.elems[ptr.index], nil
}

func (arr *Array[T, U]) Set(index uint64, elem T) error {
	if err := arr.SetMem(index, elem); err != nil {
		return err
	}
	return arr.writeAll()
}

func (arr *Array[T, U]) SetMem(index uint64, elem T) (error) {
	ptr := arr.pointer(index)
	if !arr.isValid(ptr) {
		return ErrOutOfBounds
	}

	p, err := arr.fetch(ptr.pageId)
	if err != nil {
		return err
	}

	p.dirty = true
	p.elems[ptr.index] = elem
	return nil
}

func (arr *Array[T, U]) Size() uint64 {
	return arr.meta.size
}

func (arr *Array[T, U]) Truncate(size uint64) error {
	if size == arr.meta.size {
		return nil
	}

	ptr := arr.pointer(size - 1)
	cap := arr.cap()

	if size > arr.meta.size {
		if size <= cap {
			return nil
		}

		_, err := arr.pager.Alloc(int(ptr.pageId - arr.pager.Count() + 1))
		if err != nil {
			return err
		}

		return nil
	}

	if size > cap - arr.elemsPerPage() {
		arr.meta.dirty = true
		arr.meta.size = size
		return nil
	}

	shrinkCount := int(arr.pager.Count() - ptr.pageId - 1)
	err := arr.pager.Free(shrinkCount)
	if err != nil {
		return err
	}

	for i := 0; i < shrinkCount; i++ {
		delete(arr.pages, arr.pager.Count() - uint64(i) - 1)
	}

	arr.meta.dirty = true
	arr.meta.size = size

	return nil
}

func (arr *Array[T, U]) Scan(index uint64, reverse bool, scanFn func(index uint64, elem T) (bool, error)) error {
	if arr.meta.size == 0 {
		return nil
	}

	ptr := arr.pointer(index)
	if !arr.isValid(ptr) {
		return ErrOutOfBounds
	}

	var lastPageId uint64
	var incr int
	if reverse {
		lastPageId = ptr.pageId
		incr = -1
	} else {
		lastPageId = arr.pager.Count() - ptr.pageId
		incr = 1
	}

	n := 0
	L: for pageId := ptr.pageId; pageId <= lastPageId; pageId += uint64(incr) {
		p, err := arr.fetch(pageId)
		if err != nil {
			return err
		}

		for index, elem := range p.elems {
			n++
			stop, err := scanFn(
				arr.index(&pointer{
					pageId: pageId,
					index:  uint16(index),
				}),
				elem,
			)
			if err != nil {
				return err
			} else if stop {
				return nil
			}

			if n >= int(arr.meta.size) {
				break L
			}
		}
	}
	return nil
}

func (arr *Array[T, U]) WriteAll() error {
	return arr.writeAll()
}

func (arr *Array[T, U]) Close() error {
	if err := arr.writeAll(); err != nil {
		return err
	}
	return arr.pager.Close()
}

func (arr *Array[T, U]) Print() error {
	if err := arr.Scan(0, false, func(index uint64, elem T) (bool, error) {
		fmt.Println(index, *elem)
		return false, nil
	}); err != nil {
		return err
	}
	fmt.Println("meta", arr.meta)
	return nil
}

func (arr *Array[T, U]) fetch(id uint64) (*page[T, U], error) {
	page, found := arr.pages[id]
	if found {
		return page, nil
	}

	page = newPage[T, U](id, arr.meta)
	if err := arr.pager.Unmarshal(id, page); err != nil {
		return nil, err
	}

	page.dirty = false
	arr.pages[page.id] = page

	return page, nil
}

func (arr *Array[T, U]) lastPage() (*page[T, U], error) {
	var pid uint64 = 0
	ptr := arr.pointer(arr.Size() - 1)

	if arr.isValid(ptr) {
		pid = ptr.pageId
	} else if arr.pager.Count() > 1 {
		pid = 1
	}

	if pid != 0 {
		p, err := arr.fetch(pid)
		if err != nil {
			return nil, err
		}
	
		if !arr.isFull(p) {
			if len(p.elems) >= int(ptr.index)+1 {
				p.dirty = true
				p.elems = p.elems[:ptr.index+1]
			}
			return p, nil
		}
	}

	pid, err := arr.pager.Alloc(1)
	if err != nil {
		return nil, err
	}
	
	p, err := arr.fetch(pid)
	if err != nil {
		return nil, err
	}

	if len(p.elems) >= int(ptr.index)+1 {
		p.dirty = true
		p.elems = p.elems[:ptr.index+1]
	}
	return p, nil
}

func (arr *Array[T, U]) cap() uint64 {
	return (arr.pager.Count() - 1) * arr.elemsPerPage()
}

func (arr *Array[T, U]) elemsPerPage() uint64 {
	return uint64((arr.meta.pageSize - pageHeaderSize) / arr.meta.elemSize)
}

func (arr *Array[T, U]) pointer(index uint64) *pointer {
	elemsPerPage := arr.elemsPerPage()
	return &pointer{
		pageId: index / elemsPerPage + 1,
		index:  uint16(index % elemsPerPage),
	}
}

func (arr *Array[T, U]) index(ptr *pointer) uint64 {
	return (ptr.pageId - 1) * arr.elemsPerPage() + uint64(ptr.index)
}

func (arr *Array[T, U]) isValid(ptr *pointer) bool {
	return arr.index(ptr) < arr.meta.size
}

func (arr *Array[T, U]) isFull(p *page[T, U]) bool {
	return len(p.elems) == int(arr.elemsPerPage())
}

func (arr *Array[T, U]) open(opts *ArrayOptions) error {
	if arr.pager.Count() == 0 {
		err := arr.init(opts)
		if err != nil {
			return err
		}
		return arr.pager.Marshal(0, arr.meta)
	}

	arr.meta = &metadata{
		pageSize: uint16(arr.pager.PageSize()),
	}
	return arr.pager.Unmarshal(0, arr.meta)
}

func (arr *Array[T, U]) init(opts *ArrayOptions) error {
	arr.meta = &metadata{
		dirty:    true,
		size:     0,
		pageSize: opts.PageSize,
		preAlloc: opts.PreAlloc,
		elemSize: opts.ElemSize,
	}

	_, err := arr.pager.Alloc(1 + int(opts.PreAlloc))
	return err
}

func (arr *Array[T, U]) writeAll() error {
	if arr.pager.ReadOnly() {
		return nil
	}

	for _, p := range arr.pages {
		if p.dirty {
			if err := arr.pager.Marshal(uint64(p.id), p); err != nil {
				return err
			}
			p.dirty = false
		}
	}

	return arr.writeMeta()
}

func (arr *Array[T, U]) writeMeta() error {
	if arr.meta.dirty {
		err := arr.pager.Marshal(0, arr.meta)
		arr.meta.dirty = false
		return err
	}

	return nil
}
