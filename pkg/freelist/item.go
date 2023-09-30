package freelist

type value struct {
	pageId    uint64
	freeSpace uint16
}

type item struct {
	val  *value
	self *Pointer
	next *Pointer
	prev *Pointer
}

func (i *item) isHead() bool {
	return i.prev == nil
}

func (i *item) isTail() bool {
	return i.next == nil
}

func (i *item) setPrev(itm *item) {
	if itm == nil {
		i.prev = nil
		return
	}
	itm.next = i.self
	i.prev = itm.self
}

func (i *item) setNext(itm *item) {
	if itm == nil {
		i.next = nil
		return
	}
	i.next = itm.self
	itm.prev = i.self
}

func (i *item) setBetween(left, right *item) {
	i.setPrev(left)
	i.setNext(right)
}
