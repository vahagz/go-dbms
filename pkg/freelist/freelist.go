package freelist

type Allocator interface {
	Alloc(n int) (uint64, error)
}

type RemoveFunc func(pageId uint64, freeSpace uint16) bool

type Freelist interface {
	Alloc(n int) ([]uint64, error)
	SetAllocator(a Allocator)
	WriteAll() error
	Close() error
	Print() error
}

type LinkedFreelist struct {
	linkedList     LinkedList
	targetPageSize uint16
	allocator      Allocator
	removeFunc     RemoveFunc
}

func (fl *LinkedFreelist) SetAllocator(a Allocator) {
	fl.allocator = a
}

// func (fl *freelist) set(targetItem *item, freeSpace uint16) error {
// 	var err error

// 	targetPage, targetItem, err := fl.getItem(targetItem.self)
// 	if err != nil {
// 		return err
// 	}

// 	var prevPage *page
// 	var prevItem *item
// 	if targetItem.prev != nil {
// 		prevPage, prevItem, err = fl.getItem(targetItem.prev)
// 		if err != nil {
// 			return err
// 		}
// 	}

// 	var nextPage *page
// 	var nextItem *item
// 	if targetItem.next != nil {
// 		nextPage, nextItem, err = fl.getItem(targetItem.next)
// 		if err != nil {
// 			return err
// 		}
// 	}

// 	targetPage.dirty = true

// 	// check if item must be removed
// 	if freeSpace == 0 ||
// 		(fl.removeFunc != nil && fl.removeFunc(targetItem.val.pageId, targetItem.val.freeSpace)) {
// 		// linking left and right items to each other
// 		if prevItem != nil {
// 			prevPage.dirty = true
// 			prevItem.setNext(nextItem)
// 		}
// 		if nextItem != nil {
// 			nextPage.dirty = true
// 			nextItem.setPrev(prevItem)
// 		}

// 		// if removing head, meta must be updated
// 		if prevItem == nil {
// 			fl.meta.dirty = true
// 			fl.meta.head = targetItem.next
// 		}

// 		return fl.removeItem(targetItem.self)
// 	}
// 	// check if moving item is not required
// 	if (prevItem == nil || prevItem.val.freeSpace <= freeSpace) &&
// 		(nextItem == nil || freeSpace <= nextItem.val.freeSpace) {
// 		targetItem.val.freeSpace = freeSpace
// 		return nil
// 	}

// 	var startPtr *Pointer
// 	if nextItem != nil && nextItem.val.freeSpace < freeSpace {
// 		settled := false
// 		leftPage := nextPage
// 		leftItem := nextItem
// 		if nextItem != nil {
// 			startPtr = nextItem.next
// 		}
// 		err := fl.scan(startPtr, false, func(p *page, itm *item) (bool, error) {
// 			if freeSpace <= itm.val.freeSpace {
// 				p.dirty = true
// 				if leftPage != nil {
// 					leftPage.dirty = true
// 				}
// 				targetItem.setBetween(leftItem, itm)
// 				settled = true
// 				return true, nil
// 			}
// 			leftPage = p
// 			leftItem = itm
// 			return false, nil
// 		})
// 		if err != nil {
// 			return err
// 		}

// 		// if not settled, that menas target must be
// 		// set to most right place (tail) in linked list
// 		if !settled {
// 			targetItem.setBetween(leftItem, nil)
// 		}

// 		// if target is head, meta must be updated
// 		if prevItem == nil {
// 			nextItem.setPrev(nil)
// 			fl.meta.dirty = true
// 			fl.meta.head = nextItem.self
// 		}
// 	} else if prevItem != nil && freeSpace < prevItem.val.freeSpace { // target item must be moved to left
// 		settled := false
// 		rightPage := prevPage
// 		rightItem := prevItem
// 		if prevItem != nil {
// 			startPtr = prevItem.prev
// 		}

// 		err := fl.scan(startPtr, true, func(p *page, itm *item) (bool, error) {
// 			if freeSpace >= itm.val.freeSpace {
// 				p.dirty = true
// 				if rightPage != nil {
// 					rightPage.dirty = true
// 				}
// 				targetItem.setBetween(itm, rightItem)
// 				settled = true
// 				return true, nil
// 			}
// 			rightPage = p
// 			rightItem = itm
// 			return false, nil
// 		})
// 		if err != nil {
// 			return err
// 		}

// 		// if not settled, that menas that target must
// 		// be set to most left place (head) in linked list
// 		if !settled {
// 			_, head, err := fl.getItem(fl.meta.head)
// 			if err != nil {
// 				return err
// 			}

// 			targetItem.setBetween(nil, head)
// 			fl.meta.dirty = true
// 			fl.meta.head = targetItem.self
// 		}
// 	}

// 	// linking prev and next items of target to each other
// 	if prevItem != nil {
// 		prevPage.dirty = true
// 		prevItem.setNext(nextItem)
// 	}
// 	if nextItem != nil {
// 		nextPage.dirty = true
// 		nextItem.setPrev(prevItem)
// 	}
// 	targetItem.val.freeSpace = freeSpace

// 	return nil
// }

// func (fl *freelist) find(
// 	size uint16,
// ) (
// 	leftPage *page,
// 	leftItem *item,
// 	rightPage *page,
// 	rightItem *item,
// 	err error,
// ) {
// 	err = fl.scan(nil, false, func(p *page, itm *item) (bool, error) {
// 		if itm.val.freeSpace >= size {
// 			rightPage = p
// 			rightItem = itm
// 			return true, nil
// 		}
// 		leftPage = p
// 		leftItem = itm
// 		return false, nil
// 	})
// 	if err != nil {
// 		return nil, nil, nil, nil, err
// 	}
// 	return leftPage, leftItem, rightPage, rightItem, nil
// }

// func (fl *freelist) initTargetPageSeq(startPid uint64, count uint16) error {
// 	for i := 0; i < int(count); i++ {
// 		_, err := fl.AddMem(startPid+uint64(i), fl.targetPageSize)
// 		if err != nil {
// 			return err
// 		}
// 	}
// 	return nil
// }

// func (fl *freelist) removeItem(ptr *Pointer) error {
// 	p, err := fl.fetch(ptr.PageId)
// 	if err != nil {
// 		return err
// 	}

// 	p.dirty = true
// 	p.free = append(p.free, ptr.Index)
// 	delete(p.items, ptr.Index)

// 	var notFullMeta *metadata
// 	found := false
// 	err = fl.scanMeta(func(meta *metadata) (bool, error) {
// 		if !meta.isFull() {
// 			notFullMeta = meta
// 		}

// 		for pageId := range meta.notFullPages {
// 			if pageId == ptr.PageId {
// 				meta.dirty = true
// 				meta.notFullPages[ptr.PageId]++
// 				found = true
// 				return true, nil
// 			}
// 		}

// 		return false, nil
// 	})
// 	if err != nil {
// 		return err
// 	}

// 	if found {
// 		return nil
// 	}
// 	if notFullMeta != nil {
// 		notFullMeta.dirty = true
// 		notFullMeta.notFullPages[ptr.PageId] = 1
// 		return nil
// 	}

// 	m, err := fl.extendMeta()
// 	if err != nil {
// 		return err
// 	}

// 	m.dirty = true
// 	m.notFullPages[ptr.PageId] = 1
// 	return nil
// }
