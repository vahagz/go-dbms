package freelist

import (
	"encoding/binary"
	"fmt"

	"go-dbms/pkg/pager"
)

var bin = binary.BigEndian

func Open(fileName string, opts *LinkedListOptions) (*LinkedList, error) {
	p, err := pager.Open(fileName, int(opts.PageSize), false, 0664)
	if err != nil {
		return nil, err
	}

	fl := &LinkedList{
		pager: p,
		pages: map[uint32]*page{},
		metas: map[uint32]*metadata{},
	}

	return fl, fl.open(opts)
}

type LinkedListADS interface {
	Push(val []byte) (PTR, error)
	PushMem(val []byte) (PTR, error)
	Del(ptr PTR) error
	DelMem(ptr PTR) error
	Pop(n int) ([]PTR, [][]byte, error)
	PopMem(n int) ([]PTR, [][]byte, error)
	WriteAll() error
}

type LinkedList struct {
	meta  *metadata
	pager *pager.Pager
	pages map[uint32]*page
	metas map[uint32]*metadata
}

func (fl *LinkedList) Push(val []byte) (PTR, error) {
	if ptr, err := fl.PushMem(val); err != nil {
		return nil, err
	} else {
		return ptr, fl.writeAll()
	}
}

func (fl *LinkedList) PushMem(val []byte) (PTR, error) {
	itm, err := fl.push(val)
	if err != nil {
		return nil, err
	}

	return itm.self, nil
}

func (fl *LinkedList) Del(ptr PTR) error {
	if err := fl.DelMem(ptr); err != nil {
		return err
	}
	return fl.writeAll()
}

func (fl *LinkedList) DelMem(ptr PTR) error {
	pt, ok := ptr.(*Pointer)
	if !ok {
		return fmt.Errorf("invalid pointer => %v", ptr)
	}

	_, itm, err := fl.getItem(pt)
	if err != nil {
		return err
	}

	err = fl.del(pt)
	if err != nil {
		return err
	}

	if *pt == *fl.meta.head {
		return fl.replaceHead(itm.next)
	}
	return nil
}

func (fl *LinkedList) Pop(n int) ([]PTR, [][]byte, error) {
	ptrs, vals, err := fl.pop(n)
	if err != nil {
		return nil, nil, err
	}
	return ptrs, vals, fl.writeAll()
}

func (fl *LinkedList) PopMem(n int) ([]PTR, [][]byte, error) {
	return fl.pop(n)
}

func (fl *LinkedList) WriteAll() error {
	return fl.writeAll()
}

func (fl *LinkedList) Close() error {
	err := fl.writeAll()
	if err != nil {
		return err
	}
	return fl.pager.Close()
}

func (fl *LinkedList) Print() error {
	err := fl.scan(nil, false, func(p *page, itm *item) (bool, error) {
		fmt.Printf("ptr -> %v, val -> %v, prev -> %v, next -> %v\n", itm.self, itm.val, itm.prev, itm.next)
		return false, nil
	})
	if err != nil {
		return err
	}

	fmt.Println("head", fl.meta.head)
	return fl.scanMeta(func(meta *metadata) (bool, error) {
		fmt.Println("pages", fl.meta.notFullPages)
		return false, nil
	})
}

func (fl *LinkedList) pop(n int) ([]PTR, [][]byte, error) {
	ptrs := []PTR{}
	vals := [][]byte{}

	var lastItem *item
	fl.scan(nil, false, func(p *page, itm *item) (bool, error) {
		lastItem = itm
		ptrs = append(ptrs, itm.self)
		vals = append(vals, itm.val)
		n--
		return n == 0, nil
	})

	for _, ptr := range ptrs {
		err := fl.del(ptr.(*Pointer))
		if err != nil {
			return nil, nil, err
		}
	}

	return ptrs, vals, fl.replaceHead(lastItem.next)
}

func (fl *LinkedList) del(ptr *Pointer) error {
	p, err := fl.fetch(ptr.PageId)
	if err != nil {
		return err
	}

	itm := p.items[ptr.Index]

	prevPage, prevItem, err := fl.getItem(itm.prev)
	if err != nil {
		return err
	}

	nextPage, nextItem, err := fl.getItem(itm.next)
	if err != nil {
		return err
	}

	p.dirty = true

	if prevItem != nil {
		prevPage.dirty = true
		prevItem.setNext(nextItem)
	}
	if nextItem != nil {
		nextPage.dirty = true
		nextItem.setPrev(prevItem)
	}

	p.del(ptr.Index)

	return nil
}

func (fl *LinkedList) getNotFullPage() (*page, *metadata, error) {
	var err error
	var page *page
	var pMeta *metadata
	err = fl.scanMeta(func(meta *metadata) (bool, error) {
		for pageId := range meta.notFullPages {
			page, err = fl.fetch(pageId)
			pMeta = meta
			return true, err
		}
		return false, nil
	})
	if err != nil {
		return nil, nil, err
	}

	if page != nil {
		return page, pMeta, nil
	}

	pages, metas, err := fl.alloc(fl.meta.preAlloc)
	if err != nil {
		return nil, nil, err
	}

	return pages[0], metas[0], nil
}

func (fl *LinkedList) alloc(n uint16) ([]*page, []*metadata, error) {
	pid, err := fl.pager.Alloc(int(n))
	if err != nil {
		return nil, nil, err
	}

	return fl.initPageSeq(uint32(pid), n)
}

func (fl *LinkedList) push(val []byte) (*item, error) {
	p, meta, err := fl.getNotFullPage()
	if err != nil {
		return nil, err
	}

	itm, err := p.add(val)
	if err != nil {
		return nil, err
	}

	meta.updatePageFreeCount(p)

	return itm, fl.setHead(itm)
}

func (fl *LinkedList) replaceHead(ptr *Pointer) error {
	p, head, err := fl.getItem(ptr)
	if err != nil {
		return err
	}

	p.dirty = true
	fl.meta.dirty = true
	head.prev = nil
	fl.meta.head = head.self
	return nil
}

func (fl *LinkedList) setHead(itm *item) error {
	fl.meta.dirty = true

	if itm == nil {
		fl.meta.head = nil
		return nil
	}
	if fl.meta.head == nil {
		fl.meta.head = itm.self
		return nil
	}

	page, head, err := fl.getItem(fl.meta.head)
	if err != nil {
		return err
	}

	fl.meta.head = itm.self
	itm.setNext(head)
	page.dirty = true
	return nil
}

func (fl *LinkedList) scan(start *Pointer, reverse bool, scanFn func(p *page, itm *item) (bool, error)) error {
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

func (fl *LinkedList) getItem(ptr *Pointer) (*page, *item, error) {
	if ptr == nil {
		return nil, nil, nil
	}

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

func (fl *LinkedList) fetch(id uint32) (*page, error) {
	page, found := fl.pages[id]
	if found {
		return page, nil
	}

	page = newPage(id, fl.meta.pageSize, fl.meta.valSize)
	if err := fl.pager.Unmarshal(uint64(id), page); err != nil {
		return nil, err
	}

	page.dirty = false
	fl.pages[page.id] = page

	return page, nil
}

func (fl *LinkedList) addPagesToMeta(pages []*page) ([]*metadata, error) {
	metas := make([]*metadata, len(pages))
	i := 0
	err := fl.scanMeta(func(meta *metadata) (bool, error) {
		for !meta.isFull() && i < len(pages) {
			meta.dirty = true
			meta.notFullPages[pages[i].id] = uint16(len(pages[i].free))
			metas[i] = meta
			i++
		}
		return i == len(pages), nil
	})
	if err != nil {
		return nil, err
	}

	if i == len(pages) {
		return metas, nil
	}

	extraMeta, err := fl.extendMeta()
	if err != nil {
		return nil, err
	}
	for i < len(pages) {
		if extraMeta.isFull() {
			extraMeta, err = fl.extendMeta()
			if err != nil {
				return nil, err
			}
		}

		extraMeta.dirty = true
		extraMeta.notFullPages[pages[i].id] = uint16(len(pages[i].free))
		metas[i] = extraMeta
		i++
	}
	return metas, nil
}

func (fl *LinkedList) extendMeta() (*metadata, error) {
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

func (fl *LinkedList) fetchMeta(id uint32, pageSize uint16) (*metadata, error) {
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

func (fl *LinkedList) scanMeta(scanFn func(meta *metadata) (bool, error)) error {
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

func (fl *LinkedList) initPageSeq(startPid uint32, count uint16) ([]*page, []*metadata, error) {
	pages := make([]*page, count)
	for i := 0; i < int(count); i++ {
		id := startPid + uint32(i)
		p := newPage(id, fl.meta.pageSize, fl.meta.valSize)
		p.init()
		pages[i] = p
		fl.pages[id] = p
	}

	metas, err := fl.addPagesToMeta(pages)
	return pages, metas, err
}

func (fl *LinkedList) open(opts *LinkedListOptions) error {
	if fl.pager.Count() == 0 {
		err := fl.init(opts)
		if err != nil {
			return err
		}
		return fl.pager.Marshal(0, fl.meta)
	}

	var err error
	fl.meta, err = fl.fetchMeta(0, opts.PageSize)
	return err
}

func (fl *LinkedList) init(opts *LinkedListOptions) error {
	fl.meta = &metadata{
		dirty:        true,
		pageSize:     opts.PageSize,
		notFullPages: map[uint32]uint16{},
		preAlloc:     uint16(opts.PreAlloc),
		valSize:      opts.ValSize,
	}

	_, err := fl.pager.Alloc(1 + int(opts.PreAlloc))
	if err != nil {
		return err
	}

	_, _, err = fl.initPageSeq(1, uint16(opts.PreAlloc))
	return err
}

func (fl *LinkedList) writeAll() error {
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

func (fl *LinkedList) writeMeta() error {
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
