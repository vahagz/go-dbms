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
	AddMem(pageId uint64, freeSpace uint16) (*Pointer, error)
	Get(ptr *Pointer) (uint16, error)
	Set(ptr *Pointer, freeSpace uint16) error
	Del(ptr *Pointer) error
	Fit(size uint16) (uint64, *Pointer, error)
	SetRemoveFunc(fn func(pageId uint64, freeSpace uint16) bool)
	Flush() error
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

	removeFunc     func(pageId uint64, freeSpace uint16) bool
}

func (fl *freelist) Add(pageId uint64, freeSpace uint16) (*Pointer, error) {
	ptr, err := fl.AddMem(pageId, freeSpace)
	if err != nil {
		return nil, err
	}

	return ptr, fl.writeAll()
}

func (fl *freelist) AddMem(pageId uint64, freeSpace uint16) (*Pointer, error) {
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

	itmPtr, err := fl.findFreePage(itm)
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

	return itmPtr, nil
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

func (fl *freelist) Del(ptr *Pointer) error {
	return fl.Set(ptr, 0)
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

func (fl *freelist) SetRemoveFunc(fn func(pageId uint64, freeSpace uint16) bool) {
	fl.removeFunc = fn
}

func (fl *freelist) Flush() error {
	return fl.writeAll()
}

func (fl *freelist) Close() error {
	err := fl.writeAll()
	if err != nil {
		return err
	}
	return fl.pager.Close()
}

func (fl *freelist) Print() error {
	// err := fl.scan(nil, false, func(p *page, itm *item) (bool, error) {
	// 	fmt.Printf("ptr -> %v, val -> %v, prev -> %v, next -> %v\n", itm.self, itm.val, itm.prev, itm.next)
	// 	return false, nil
	// })
	// if err != nil {
	// 	return err
	// }

	fmt.Println("head", fl.meta.head)
	return fl.scanMeta(func(meta *metadata) (bool, error) {
		fmt.Println("pages", fl.meta.notFullPages)
		return false, nil
	})
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

	// check if item must be removed
	if  freeSpace == 0 ||
			(fl.removeFunc != nil && fl.removeFunc(targetItem.val.pageId, targetItem.val.freeSpace)) {
		// linking left and right items to each other
		if prevItem != nil {
			prevPage.dirty = true
			prevItem.setNext(nextItem)
		}
		if nextItem != nil {
			nextPage.dirty = true
			nextItem.setPrev(prevItem)
		}

		// if removing head, meta must be updated
		if prevItem == nil {
			fl.meta.dirty = true
			fl.meta.head = targetItem.next
		}

		return fl.removeItem(targetItem.self)
	}
	// check if moving item is not required
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

		// if not settled, that menas target must be
		// set to most right place (tail) in linked list
		if !settled {
			targetItem.setBetween(leftItem, nil)
		}

		// if target is head, meta must be updated
		if prevItem == nil {
			nextItem.setPrev(nil)
			fl.meta.dirty = true
			fl.meta.head = nextItem.self
		}
	} else if prevItem != nil && freeSpace < prevItem.val.freeSpace { // target item must be moved to left
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

		// if not settled, that menas that target must
		// be set to most left place (head) in linked list
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

	// linking prev and next items of target to each other
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

func (fl *freelist) findFreePage(itm *item) (*Pointer, error) {
	var ptr *Pointer
	err := fl.scanMeta(func(meta *metadata) (bool, error) {
		for pageId := range meta.notFullPages {
			page, err := fl.fetch(pageId)
			if err != nil {
				return false, err
			}

			ptr, err = page.addItem(itm)
			if err != nil {
				return false, err
			}

			meta.dirty = true
			meta.notFullPages[pageId]--
			if meta.notFullPages[pageId] == 0 {
				delete(meta.notFullPages, pageId)
			}

			return true, nil
		}
		return false, nil
	})
	if err != nil {
		return nil, err
	} else if ptr != nil {
		return ptr, nil
	}

	pid, err := fl.pager.Alloc(int(fl.meta.preAlloc))
	if err != nil {
		return nil, err
	}

	_, err = fl.initPageSeq(uint32(pid), fl.meta.preAlloc)
	if err != nil {
		return nil, err
	}

	return fl.findFreePage(itm)
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

func (fl *freelist) removeItem(ptr *Pointer) error {
	p, err := fl.fetch(ptr.PageId)
	if err != nil {
		return err
	}

	p.dirty = true
	p.free = append(p.free, ptr.Index)
	delete(p.items, ptr.Index)

	var notFullMeta *metadata
	found := false
	err = fl.scanMeta(func(meta *metadata) (bool, error) {
		if !meta.isFull() {
			notFullMeta = meta
		}

		for pageId := range meta.notFullPages {
			if pageId == ptr.PageId {
				meta.dirty = true
				meta.notFullPages[ptr.PageId]++
				found = true
				return true, nil
			}
		}

		return false, nil
	})
	if err != nil {
		return err
	}

	if found {
		return nil
	}
	if notFullMeta != nil {
		notFullMeta.dirty = true
		notFullMeta.notFullPages[ptr.PageId] = 1
		return nil
	}

	m, err := fl.extendMeta()
	if err != nil {
		return err
	}

	m.dirty = true
	m.notFullPages[ptr.PageId] = 1
	return nil
}

func (fl *freelist) addPagesToMeta(pages []*page, freeCount uint16) error {
	i := 0
	err := fl.scanMeta(func(meta *metadata) (bool, error) {
		for !meta.isFull() && i < len(pages) {
			meta.dirty = true
			meta.notFullPages[pages[i].id] = freeCount
			i++
		}
		return i == len(pages), nil
	})
	if err != nil {
		return err
	}

	if i == len(pages) {
		return nil
	}

	extraMeta, err := fl.extendMeta()
	if err != nil {
		return err
	}
	for i < len(pages) {
		if extraMeta.isFull() {
			extraMeta, err = fl.extendMeta()
			if err != nil {
				return err
			}
		}

		extraMeta.dirty = true
		extraMeta.notFullPages[pages[i].id] = freeCount
		i++
	}
	return nil
}

func (fl *freelist) extendMeta() (*metadata, error) {
	m := &metadata{
		dirty:        true,
		pageSize:     fl.meta.pageSize,
		notFullPages: map[uint32]uint16{},
		next:         fl.meta.next,
	}

	pid, err := fl.pager.Alloc(1)
	if err != nil {
		return nil, err
	}

	fl.metas[uint32(pid)] = m
	fl.meta.dirty = true
	fl.meta.next = uint32(pid)
	return m, nil
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

func (fl *freelist) scanMeta(scanFn func(meta *metadata) (bool, error)) error {
	var stop bool
	var err error
	meta := fl.meta
	for meta != nil {
		if stop, err = scanFn(meta); err != nil {
			return err
		} else if stop {
			return nil
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

func (fl *freelist) initPageSeq(startPid uint32, count uint16) ([]*page, error) {
	pages := make([]*page, count)
	for i := 0; i < int(count); i++ {
		id := startPid + uint32(i)
		p := newPage(id, fl.meta.pageSize)
		p.init()
		pages[i] = p
		fl.pages[id] = p
	}
	return pages, fl.addPagesToMeta(pages, fl.meta.pageSize / itemSize)
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
		preAlloc:     uint16(opts.PreAlloc),
	}

	_, err := fl.pager.Alloc(1 + opts.PreAlloc)
	if err != nil {
		return err
	}

	_, err = fl.initPageSeq(1, uint16(opts.PreAlloc))
	return err
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
