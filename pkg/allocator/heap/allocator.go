package allocator

import (
	"fmt"
	"go-dbms/pkg/pager"
	"go-dbms/pkg/rbtree"

	"github.com/pkg/errors"
)

func Open(filename string, opts *Options) (*Allocator, error) {
	freelist, err := rbtree.Open[*freelistKey, *rbtree.DummyVal](
		fmt.Sprintf("%s_freelist.bin", filename),
		&rbtree.Options{
			PageSize: opts.TreePageSize,
		},
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open rbtree for allocator")
	}

	a := &Allocator{
		freelist:       freelist,
		targetPageSize: opts.TargetPageSize,
		pager:          opts.Pager,
	}

	return a, a.init()
}

type Allocator struct {
	freelist       *rbtree.RBTree[*freelistKey, *rbtree.DummyVal]
	targetPageSize uint16
	pager          *pager.Pager
	meta           *metadata
	metaPtr        *Pointer
}

func (a *Allocator) Alloc(size uint32) Pointable {
	requiredSize := size + 2 * PointerMetaSize
	entry, err := a.freelist.Get(&freelistKey{0, requiredSize})
	if err != nil && err != rbtree.ErrNotFound {
		panic(errors.Wrap(err, "failed to find free space from freelist"))
	} else if entry != nil {
		shrinkSize := requiredSize
		if entry.Key.size - shrinkSize <= 2 * PointerMetaSize {
			shrinkSize = entry.Key.size
		}

		ptr := a.createPointer(entry.Key.ptr + PointerMetaSize, shrinkSize - 2 * PointerMetaSize)
		ptr.meta.free = false
		if err := a.shrink(entry, shrinkSize); err != nil {
			panic(errors.Wrap(err, "failed to shrink allocated space"))
		} else if err := ptr.writeMeta(); err != nil {
			panic(errors.Wrap(err, "failed to update allocated Pointer meta"))
		}

		return ptr
	}

	requiredTotalSize := a.meta.top + uint64(requiredSize)
	requiredExtraPages := requiredTotalSize / uint64(a.targetPageSize) + 1
	if requiredExtraPages > a.pager.Count() {
		_, err := a.pager.Alloc(int(requiredExtraPages - a.pager.Count()))
		if err != nil {
			panic(errors.Wrap(err, "failed to alloc space"))
		}
	}

	ptr := a.newPointer(size)
	ptr.meta.free = false
	if err := ptr.writeMeta(); err != nil {
		panic(errors.Wrap(err, "failed to write new pointers meta"))
	}/* else if err := a.writeMeta(); err != nil {
		panic(errors.Wrap(err, "failed to update allocator meta"))
	}*/

	return ptr
}

func (a *Allocator) Free(p Pointable) {
	ptr, ok := p.(*Pointer)
	if !ok {
		panic(errors.New("invalid Pointer type"))
	}

	if ptr.ptr + uint64(ptr.meta.size) + PointerMetaSize < a.meta.top {
		if nextPtr, err := ptr.next(); err != nil {
			panic(errors.Wrap(err, "failed to get freed ptr next ptr"))
		} else if nextPtr.meta.free {
			ptr.meta.size += nextPtr.meta.size + 2 * PointerMetaSize
			if err := a.freelist.Delete(nextPtr.key()); err != nil {
				panic(errors.Wrap(err, "failed to delete freelist item"))
			}
		}
	}

	if ptr.ptr - PointerMetaSize > metadataSize + 2 * PointerMetaSize {
		if prevPtr, err := ptr.prev(); err != nil {
			panic(errors.Wrap(err, "failed to get freed ptr prev ptr"))
		} else if prevPtr.meta.free {
			ptr.ptr = prevPtr.ptr
			ptr.meta.size += prevPtr.meta.size + 2 * PointerMetaSize
			if err := a.freelist.Delete(prevPtr.key()); err != nil {
				panic(errors.Wrap(err, "failed to delete freelist item"))
			}
		}
	}

	if ptr.ptr + uint64(ptr.meta.size) + PointerMetaSize == a.meta.top {
		a.meta.top -= uint64(ptr.meta.size) + 2 * PointerMetaSize
		return
	}

	ptr.meta.free = true
	err := a.freelist.Insert(&rbtree.Entry[*freelistKey, *rbtree.DummyVal]{
		Key: ptr.key(),
		Val: &rbtree.DummyVal{},
	})
	if err != nil {
		panic(errors.Wrap(err, "failed to insert ptr to freelist"))
	} else if err := ptr.writeMeta(); err != nil {
		panic(errors.Wrap(err, "failed to update freed ptr meta"))
	}
}

func (a *Allocator) Scan(startPtr Pointable, scanFn func(Pointable) (bool, error)) (err error) {
	var ptr *Pointer

	if startPtr == nil {
		startPtr = a.metaPtr
	}

	ptr = startPtr.(*Pointer)
	ptr, err = ptr.next()
	if err != nil {
		return err
	}

	cnt := true
	for cnt {
		cnt = ptr.Addr() + uint64(ptr.Size()) + PointerMetaSize < a.meta.top
		if stop, err := scanFn(ptr); err != nil {
			return err
		} else if stop {
			return nil
		} else if ptr, err = ptr.next(); err != nil {
			return err
		}
	}

	return nil
}

func (a *Allocator) Link(ptr Pointable) {
	p := ptr.(*Pointer)
	p.pager = a.pager
}

func (a *Allocator) PreAlloc(size uint32) {
	a.Free(a.Alloc(size))
}

func (a *Allocator) Size() uint64 {
	return a.meta.top
}

func (a *Allocator) FirstPointer(size uint32) Pointable {
	return a.createPointer(metadataSize + 3 * PointerMetaSize, size)
}

func (a *Allocator) Pointer(addr uint64, size uint32) Pointable {
	return a.createPointer(addr, size)
}

func (a *Allocator) Nil() Pointable {
	return a.Pointer(0, 0)
}

func (a *Allocator) Print() error {
	if err := a.freelist.Print(5); err != nil {
		return errors.Wrap(err, "freelist print failed")
	}

	fmt.Println("meta", a.meta)
	return nil
}

func (a *Allocator) Close() error {
	if err := a.writeMeta(); err != nil {
		return errors.Wrap(err, "failed to close page id RBT")
	} else if err := a.freelist.Close(); err != nil {
		return errors.Wrap(err, "failed to close free space RBT")
	} else if err := a.pager.Close(); err != nil {
		return errors.Wrap(err, "failed to close allocator pager")
	}
	return nil
}

func (a *Allocator) shrink(
	entry *rbtree.Entry[*freelistKey, *rbtree.DummyVal],
	shrinkSize uint32,
) error {
	if err := a.freelist.Delete(entry.Key); err != nil {
		return errors.Wrap(err, "failed to delete free space")
	}

	remainingSpace := entry.Key.size - shrinkSize
	if remainingSpace == 0 {
		return nil
	}

	entry.Key.ptr += uint64(shrinkSize)
	entry.Key.size -= shrinkSize
	if err := a.freelist.Insert(entry); err != nil {
		return errors.Wrap(err, "failed to insert entry key after shrink")
	}

	ptr := a.createPointer(entry.Key.ptr + PointerMetaSize, entry.Key.size - 2 * PointerMetaSize)
	ptr.meta.free = true
	return errors.Wrap(ptr.writeMeta(), "failed to update shrinked free block meta")
}

func (a *Allocator) init() error {
	a.meta = &metadata{top: 0}
	a.metaPtr = a.createPointer(PointerMetaSize, metadataSize)

	if a.pager.Count() > 0 {
		a.meta.top += PointerMetaSize
		return a.metaPtr.Get(a.meta)
	}

	a.Alloc(metadataSize)
	return nil
}

func (a *Allocator) writeMeta() error {
	return a.metaPtr.Set(a.meta)
}

func (a *Allocator) createPointer(ptr uint64, size uint32) *Pointer {
	return &Pointer{
		ptr:   ptr,
		meta:  &pointerMetadata{free: false, size: size},
		pager: a.pager,
	}
}

func (a *Allocator) newPointer(size uint32) *Pointer {
	ptr := a.createPointer(a.meta.top + PointerMetaSize, size)
	a.meta.top += uint64(size) +  2 * PointerMetaSize
	return ptr
}
