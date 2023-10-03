package array

import (
	"encoding/binary"
	"errors"
	"fmt"
	"go-dbms/pkg/pager"
	"go-dbms/util/helpers"
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
	Pop() (T, error)
	PopMem() (T, error)
	Set(index uint64, elem T) (error)
	SetMem(index uint64, elem T) (error)
	Get(index uint64) (T, error)
	Size() uint64
	Cap() uint64
	Truncate(size uint64) error
	TruncateMem(size uint64) error
	Drain() error
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
	return arr.index(&pointer{
		pageId: p.id,
		index: uint16(len(p.elems) - 1),
	}), nil
}

func (arr *Array[T, U]) Pop() (T, error) {
	elem, err := arr.PopMem()
	if err != nil {
		return nil, err
	}
	return elem, arr.writeAll()
}

func (arr *Array[T, U]) PopMem() (T, error) {
	elem, err := arr.Get(arr.Size() - 1)
	if err != nil {
		if err == ErrOutOfBounds {
			return nil, nil
		}
		return nil, err
	}

	return elem, arr.TruncateMem(arr.Size() - 1)
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
	if err := arr.TruncateMem(size); err != nil {
		return err
	}
	return arr.writeAll()
}

func (arr *Array[T, U]) TruncateMem(size uint64) error {
	if size == 0 {
		err := arr.pager.Free(int(arr.pager.Count() - 1))
		if err != nil {
			return err
		}

		arr.pages = map[uint64]*page[T, U]{}
		arr.meta.dirty = true
		arr.meta.size = size
		return nil
	}

	lastPageId := arr.pager.Count() - 1
	ptr := arr.pointer(size - 1)

	if ptr.pageId > lastPageId {
		_, err := arr.pager.Alloc(int(ptr.pageId - lastPageId))
		if err != nil {
			return err
		}

		return nil
	}

	shrinkCount := int(lastPageId - ptr.pageId)
	if shrinkCount > 0 {
		err := arr.pager.Free(shrinkCount)
		if err != nil {
			return err
		}
	}

	for i := 0; i < shrinkCount; i++ {
		delete(arr.pages, lastPageId - uint64(i))
	}

	arr.meta.dirty = true
	arr.meta.size = helpers.Min(arr.meta.size, size)
	_, err := arr.lastPage()
	return err
}

func (arr *Array[T, U]) Scan(opts *ScanOptions, scanFn func(index uint64, elem T) (bool, error)) error {
	if arr.meta.size == 0 {
		return nil
	} else if opts.Start >= arr.Size() {
		return ErrOutOfBounds
	}

	var lowerBound uint64 = 0
	var upperBound uint64 = arr.Size() - 1

	if opts.Reverse {
		opts.Start--
	}

	// L: for pageId := ptr.pageId; pageId <= lastPageId; pageId += uint64(step) {
	for i := opts.Start; i >= lowerBound && i < upperBound; {
		elem, err := arr.Get(i)
		if err != nil {
			return err
		}

		stop, err := scanFn(i, elem)
		if err != nil {
			return err
		} else if stop {
			return nil
		}

		if opts.Reverse {
			i -= opts.Step
		} else {
			i += opts.Step
		}
	}
	return nil
}

func (arr *Array[T, U]) Drain() error {
	return arr.writeAll()
}

func (arr *Array[T, U]) Close() error {
	if err := arr.writeAll(); err != nil {
		return err
	}
	return arr.pager.Close()
}

func (arr *Array[T, U]) Print() error {
	for i := uint64(0); i < arr.Size(); i++ {
		elem, err := arr.Get(i)
		if err != nil {
			return err
		}
		fmt.Println(i, *elem)
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
	var err error
	var pid uint64 = 0
	ptr := arr.pointer(arr.Size())

	if arr.isValid(ptr) {
		pid = ptr.pageId
	} else {
		pid, err = arr.pager.Alloc(1)
		if err != nil {
			return nil, err
		}
	}

	p, err := arr.fetch(pid)
	if err != nil {
		return nil, err
	}

	if len(p.elems) > int(ptr.index) {
		p.dirty = true
		p.elems = p.elems[:ptr.index]
	}
	return p, nil
}

func (arr *Array[T, U]) Cap() uint64 {
	return (arr.pager.Count() - 1) * arr.elemsPerPage()
}

func (arr *Array[T, U]) elemsPerPage() uint64 {
	var e T
	return uint64((arr.meta.pageSize - pageHeaderSize) / e.Size())
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
	return 1 <= ptr.pageId &&
		ptr.pageId < arr.pager.Count() &&
		0 <= ptr.index &&
		ptr.index < uint16(arr.elemsPerPage())
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
