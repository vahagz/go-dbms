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
	Close() error
	Print() error
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
	leftPage, leftItm, rightPage, rightItm, err := fl.find(freeSpace)
	if err != nil {
		return nil, err
	}

	itm := &item{
		val: &value{
			pageId:    pageId,
			freeSpace: freeSpace,
		},
	}

	if leftItm != nil {
		itm.prev = leftItm.self
	}
	if rightItm != nil {
		itm.next = rightItm.self
	}

	itmPtr, err := fl.add(itm)
	if err != nil {
		return nil, err
	}
	itm.self = itmPtr

	if fl.meta.head == nil || leftPage == nil {
		fl.meta.dirty = true
		fl.meta.head = itmPtr
	}
	if leftPage != nil {
		leftPage.dirty = true
		itm.setPrev(leftItm)
	}
	if rightPage != nil {
		rightPage.dirty = true
		itm.setNext(rightItm)
	}

	return itmPtr, fl.writeAll()
}

func (fl *freelist) Get(ptr *Pointer) (uint16, error) {
	_, itm, err := fl.getItem(ptr)
	if err != nil {
		return 0, err
	}
	return itm.val.freeSpace, nil
}

func (fl *freelist) Set(ptr *Pointer, freeSpace uint16) error {
	p, err := fl.fetch(ptr.PageId)
	if err != nil {
		return err
	}

	itm, ok := p.items[ptr.Index]
	if !ok {
		return fmt.Errorf("invalid pointer => %v", *ptr)
	}

	p.dirty = true
	return fl.set(itm, freeSpace)
}

func (fl *freelist) Fit(size uint16) (uint64, *Pointer, error) {
	_, _, rightPage, rightItm, err := fl.find(size)
	if err != nil {
		return 0, nil, nil
	}
	if rightPage != nil {
		rightPage.dirty = true
		err := fl.set(rightItm, rightItm.val.freeSpace - size)
		if err != nil {
			return 0, nil, err
		}

		return rightItm.val.pageId, rightItm.self, nil
	}

	pid, err := fl.allocator.Alloc(1)
	if err != nil {
		return 0, nil, nil
	}

	ptr, err := fl.Add(pid, fl.targetPageSize - size)
	if err != nil {
		return 0, nil, err
	}
	return pid, ptr, fl.writeAll()
}

func (fl *freelist) Close() error {
	err := fl.writeAll()
	if err != nil {
		return err
	}
	return fl.pager.Close()
}

func (fl *freelist) Print() error {
	fmt.Println("pages", fl.meta.notFullPages)
	fmt.Println("head", fl.meta.head)
	return fl.scan(nil, false, func(p *page, itm *item) (bool, error) {
		fmt.Printf("ptr -> %v, val -> %v, prev -> %v, next -> %v\n", itm.self, itm.val, itm.prev, itm.next)
		return false, nil
	})
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
		page.items[itmIndex] = itm

		if meta.notFullPages[pageId] == 0 {
			page.free = page.free[1:]
		}

		return true, nil
	})

	return &Pointer{
		PageId: itmPageId,
		Index:  itmIndex,
	}, err
}

func (fl *freelist) set(targetItem *item, freeSpace uint16) error {
	var err error

	targetPage, targetItem, err := fl.getItem(targetItem.self)
	if err != nil {
		return err
	}

	var prevPage *page
	var prevItem *item
	if targetItem.prev != nil {
		prevPage, prevItem, err = fl.getItem(targetItem.prev)
		if err != nil {
			return err
		}
	}

	var nextPage *page
	var nextItem *item
	if targetItem.next != nil {
		nextPage, nextItem, err = fl.getItem(targetItem.next)
		if err != nil {
			return err
		}
	}

	targetPage.dirty = true

	if  (prevItem == nil || prevItem.val.freeSpace <= freeSpace) &&
			(nextItem == nil || freeSpace <= nextItem.val.freeSpace) {
		targetItem.val.freeSpace = freeSpace
		return nil
	}

	var startPtr *Pointer
	if nextItem != nil && nextItem.val.freeSpace < freeSpace {
		settled := false
		leftPage := nextPage
		leftItem := nextItem
		if nextItem != nil {
			startPtr = nextItem.next
		}
		err := fl.scan(startPtr, false, func(p *page, itm *item) (bool, error) {
			if freeSpace <= itm.val.freeSpace {
				p.dirty = true
				if leftPage != nil {
					leftPage.dirty = true
				}
				targetItem.setBetween(leftItem, itm)
				settled = true
				return true, nil
			}
			leftPage = p
			leftItem = itm
			return false, nil
		})
		if err != nil {
			return err
		}

		if !settled {
			targetItem.setBetween(leftItem, nil)
		}
		if prevItem == nil {
			_, head, err := fl.getItem(fl.meta.head)
			if err != nil {
				return err
			}

			nextItem.setBetween(nil, head)
			fl.meta.dirty = true
			fl.meta.head = nextItem.self
		}
	} else if prevItem != nil && freeSpace < prevItem.val.freeSpace {
		settled := false
		rightPage := prevPage
		rightItem := prevItem
		if prevItem != nil {
			startPtr = prevItem.prev
		}

		err := fl.scan(startPtr, true, func(p *page, itm *item) (bool, error) {
			if freeSpace >= itm.val.freeSpace {
				p.dirty = true
				if rightPage != nil {
					rightPage.dirty = true
				}
				targetItem.setBetween(itm, rightItem)
				settled = true
				return true, nil
			}
			rightPage = p
			rightItem = itm
			return false, nil
		})
		if err != nil {
			return err
		}

		if !settled {
			_, head, err := fl.getItem(fl.meta.head)
			if err != nil {
				return err
			}

			targetItem.setBetween(nil, head)
			fl.meta.dirty = true
			fl.meta.head = targetItem.self
		}
	}

	if prevItem != nil {
		prevPage.dirty = true
		prevItem.setNext(nextItem)
	}
	if nextItem != nil {
		nextPage.dirty = true
		nextItem.setPrev(prevItem)
	}
	targetItem.val.freeSpace = freeSpace

	return nil
}

func (fl *freelist) find(
	size uint16,
) (
	leftPage *page,
	leftItem *item,
	rightPage *page,
	rightItem *item,
	err error,
) {
	err = fl.scan(nil, false, func(p *page, itm *item) (bool, error) {
		if itm.val.freeSpace >= size {
			rightPage = p
			rightItem = itm
			return true, nil
		}
		leftPage = p
		leftItem = itm
		return false, nil
	})
	if err != nil {
		return nil, nil, nil, nil, err
	}
	return leftPage, leftItem, rightPage, rightItem, nil
}

func (fl *freelist) scan(start *Pointer, reverse bool, scanFn func(p *page, itm *item) (bool, error)) error {
	ptr := fl.meta.head
	if start != nil {
		ptr = start
	}

	var err error
	var p *page
	var itm *item
	var stop bool

	for ptr != nil {
		p, itm, err = fl.getItem(ptr)
		if err != nil {
			return err
		}

		stop, err = scanFn(p, itm)
		if err != nil {
			return err
		} else if stop {
			return nil
		}

		if reverse {
			ptr = itm.prev
		} else {
			ptr = itm.next
		}
	}

	return nil
}

func (fl *freelist) getItem(ptr *Pointer) (*page, *item, error) {
	p, err := fl.fetch(ptr.PageId)
	if err != nil {
		return nil, nil, err
	}

	itm, ok := p.items[ptr.Index]
	if !ok {
		return nil, nil, fmt.Errorf("invalid pointer => %v", *ptr)
	}
	return p, itm, nil
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

func (fl *freelist) fetchMeta(id uint32, pageSize uint16) (*metadata, error) {
	meta, found := fl.metas[id]
	if found {
		return meta, nil
	}

	meta = &metadata{
		pageSize: pageSize,
	}
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

		meta, err = fl.fetchMeta(meta.next, meta.pageSize)
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
		return fl.pager.Marshal(0, fl.meta)
	}

	var err error
	fl.meta, err = fl.fetchMeta(0, opts.FreelistPageSize)
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
