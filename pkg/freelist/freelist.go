package freelist

import (
	"encoding/binary"
	"fmt"

	"go-dbms/pkg/pager"
)

var bin = binary.BigEndian

func Open(fileName string, opts *Options) (Freelist, error) {
	p, err := pager.Open(fileName, int(opts.FreelistPageSize), false, 0664)
	if err != nil {
		return nil, err
	}

	fl := &freelist{
		targetPageSize: opts.TargetPageSize,
		allocator:      opts.Allocator,
		pager:          p,
		pages:          map[uint32]*page{},
		metas:          map[uint32]*metadata{},
	}

	return fl, fl.open(opts)
}

type Allocator interface {
	Alloc(n int) (uint64, error)
}

type Freelist interface {
	Add(pageId uint64, freeSpace uint16) (*Pointer, error)
	Get(ptr *Pointer) (uint16, error)
	Set(ptr *Pointer, freeSpace uint16) error
	Fit(size uint16) (uint64, *Pointer, error)
}

type freelist struct {
	targetPageSize uint16
	allocator      Allocator
	meta           *metadata
	pager          *pager.Pager
	pages          map[uint32]*page
	metas          map[uint32]*metadata
}

func (fl *freelist) Add(pageId uint64, freeSpace uint16) (*Pointer, error) {
	left, _, _, _, err := fl.find(freeSpace)
	if err != nil {
		return nil, err
	}

	itm := &item{
		val: &value{
			pageId:    pageId,
			freeSpace: freeSpace,
		},
		next: left.next,
	}

	left.next, err = fl.add(itm)
	if err != nil {
		return nil, err
	}

	return left.next, fl.writeAll()
}

func (fl *freelist) Get(ptr *Pointer) (uint16, error) {
	itm, err := fl.getItem(ptr)
	if err != nil {
		return 0, err
	}
	return itm.val.freeSpace, nil
}

func (fl *freelist) Set(ptr *Pointer, freeSpace uint16) error {
	p, err := fl.fetch(ptr.pageId)
	if err != nil {
		return err
	}

	p.dirty = true
	p.items[ptr.index].val.freeSpace = freeSpace
	return nil
}

func (fl *freelist) Fit(size uint16) (uint64, *Pointer, error) {
	_, _, right, rightPtr, err := fl.find(size)
	if err != nil {
		return 0, nil, nil
	}
	if right != nil {
		return right.val.pageId, rightPtr, nil
	}

	pid, err := fl.allocator.Alloc(1)
	if err != nil {
		return 0, nil, nil
	}

	flPid, err := fl.Add(pid, fl.targetPageSize)
	return pid, flPid, err
}

func (fl *freelist) add(itm *item) (*Pointer, error) {
	var itmPageId uint32
	var itmIndex  uint16
	err := fl.scanMeta(func(meta *metadata, pageId uint32) (bool, error) {
		meta.dirty = true
		meta.notFullPages[pageId]--
		
		page, err := fl.fetch(pageId)
		if err != nil {
			return true, err
		}

		itmPageId = pageId
		itmIndex = page.free[0]
		page.dirty = true
		page.free = page.free[1:]
		page.items[itmIndex] = itm

		return true, nil
	})

	return &Pointer{
		pageId: itmPageId,
		index:  itmIndex,
	}, err
}

func (fl *freelist) find(
	size uint16,
) (
	left *item,
	leftPtr *Pointer,
	right *item,
	rightPtr *Pointer,
	err error,
) {
	err = fl.scan(nil, func(itm *item, ptr *Pointer) (bool, error) {
		if itm.val.freeSpace >= size {
			right = itm
			rightPtr = ptr
			return true, nil
		}
		left = itm
		leftPtr = ptr
		return false, nil
	})
	if err != nil {
		return nil, nil, nil, nil, err
	}
	return left, leftPtr, right, rightPtr, nil
}

func (fl *freelist) scan(start *Pointer, scanFn func(itm *item, ptr *Pointer) (bool, error)) error {
	ptr := fl.meta.head
	if start != nil {
		ptr = start
	}

	var err error
	var itm *item
	var stop bool

	for ptr != nil {
		itm, err = fl.getItem(ptr)
		stop, err = scanFn(itm, ptr)
		if err != nil {
			return err
		} else if stop {
			return nil
		}

		ptr = itm.next
	}

	return nil
}

func (fl *freelist) getItem(ptr *Pointer) (*item, error) {
	p, err := fl.fetch(ptr.pageId)
	if err != nil {
		return nil, err
	}

	itm, ok := p.items[ptr.index]
	if !ok {
		return nil, fmt.Errorf("invalid pointer => %v", *ptr)
	}
	return itm, nil
}

func (fl *freelist) fetch(id uint32) (*page, error) {
	page, found := fl.pages[id]
	if found {
		return page, nil
	}

	page = newPage(id, fl.meta.pageSize)
	if err := fl.pager.Unmarshal(uint64(id), page); err != nil {
		return nil, err
	}

	page.dirty = false
	fl.pages[page.id] = page

	return page, nil
}

func (fl *freelist) fetchMeta(id uint32) (*metadata, error) {
	meta, found := fl.metas[id]
	if found {
		return meta, nil
	}

	meta = &metadata{}
	err := fl.pager.Unmarshal(uint64(id), meta)
	if err != nil {
		return nil, err
	}

	fl.metas[id] = meta
	return meta, nil
}

func (fl *freelist) scanMeta(scanFn func(meta *metadata, pageId uint32) (bool, error)) error {
	var stop bool
	var err error
	meta := fl.meta
	for meta != nil {
		for pageId := range meta.notFullPages {
			if stop, err = scanFn(meta, pageId); err != nil {
				return err
			} else if stop {
				return nil
			}
		}

		if meta.next == 0 {
			break
		}

		meta, err = fl.fetchMeta(meta.next)
		if err != nil {
			return err
		}
	}

	return nil
}

func (fl *freelist) open(opts *Options) error {
	if fl.pager.Count() == 0 {
		err := fl.init(opts)
		if err != nil {
			return err
		}

		fl.meta = &metadata{}
		return fl.pager.Marshal(0, fl.meta)
	}

	var err error
	fl.meta, err = fl.fetchMeta(0)
	return err
}

func (fl *freelist) init(opts *Options) error {
	fl.meta = &metadata{
		dirty:        true,
		pageSize:     opts.FreelistPageSize,
		notFullPages: map[uint32]uint16{},
	}

	_, err := fl.pager.Alloc(1 + opts.PreAlloc)
	if err != nil {
		return err
	}
	
	for i := uint32(1); i <= uint32(opts.PreAlloc); i++ {
		fl.meta.notFullPages[i] = fl.meta.pageSize / itemSize
	}

	return nil
}

func (fl *freelist) writeAll() error {
	if fl.pager.ReadOnly() {
		return nil
	}

	for _, p := range fl.pages {
		if p.dirty {
			if err := fl.pager.Marshal(uint64(p.id), p); err != nil {
				return err
			}
			p.dirty = false
		}
	}

	return fl.writeMeta()
}

func (fl *freelist) writeMeta() error {
	if fl.meta.dirty {
		err := fl.pager.Marshal(0, fl.meta)
		fl.meta.dirty = false
		return err
	}
	
	for id, m := range fl.metas {
		if m.dirty {
			if err := fl.pager.Marshal(uint64(id), m); err != nil {
				return err
			}
			m.dirty = false
		}
	}

	return nil
}
