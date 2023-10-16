package allocator

import (
	"fmt"
	"go-dbms/pkg/pager"
	"go-dbms/pkg/rbtree"

	"github.com/pkg/errors"
)

func Open(filename string, opts *Options) (*Allocator, error) {
	freelist, err := rbtree.Open[*freelistKey, *rbtree.DummyVal](
		fmt.Sprintf("%s.bin", filename),
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
	metaPtr        *pointer
}

func (a *Allocator) Alloc(size uint32) (Pointable, error) {
	requiredSize := size + 2 * pointerMetaSize
	entry, err := a.freelist.Get(&freelistKey{0, requiredSize})
	if err != nil && err != rbtree.ErrNotFound {
		return nil, errors.Wrap(err, "failed to find free space from freelist")
	} else if entry != nil {
		shrinkSize := requiredSize
		if entry.Key.size - shrinkSize < 2 * pointerMetaSize {
			shrinkSize = entry.Key.size
		}

		ptr := a.createPointer(entry.Key.ptr + pointerMetaSize, shrinkSize - 2 * pointerMetaSize)
		if err := a.shrink(entry, shrinkSize); err != nil {
			return nil, errors.Wrap(err, "failed to shrink allocated space")
		}
		return ptr, errors.Wrap(ptr.writeMeta(), "failed to update allocated pointer meta")
	}

	requiredTotalSize := a.meta.top + uint64(requiredSize)
	requiredExtraPages := requiredTotalSize / uint64(a.targetPageSize) + 1
	if requiredExtraPages > a.pager.Count() {
		_, err := a.pager.Alloc(int(requiredExtraPages - a.pager.Count()))
		if err != nil {
			return nil, errors.Wrap(err, "failed to alloc space")
		}
	}

	ptr := a.newPointer(size)
	if err := ptr.writeMeta(); err != nil {
		return nil, errors.Wrap(err, "failed to write new pointers meta")
	}
	return ptr, errors.Wrap(a.writeMeta(), "failed to update allocator meta")
}

func (a *Allocator) Free(p Pointable) error {
	ptr, ok := p.(*pointer)
	if !ok {
		return errors.New("invalid pointer type")
	}

	if ptr.ptr + uint64(ptr.meta.size) + pointerMetaSize == a.meta.top {
		a.meta.top -= uint64(ptr.meta.size) + 2 * pointerMetaSize
		return errors.Wrap(a.writeMeta(), "faield to update meta after free")
	}

	if ptr.ptr + uint64(ptr.meta.size) + pointerMetaSize < a.meta.top {
		nextPtr, err := ptr.next()
		if err != nil {
			return errors.Wrap(err, "failed to get freed ptr next ptr")
		}

		if nextPtr.meta.free {
			ptr.meta.size += nextPtr.meta.size + 2 * pointerMetaSize
			err := a.freelist.Delete(nextPtr.key())
			if err != nil {
				return errors.Wrap(err, "failed to delete freelist item")
			}
		}
	}

	if ptr.ptr - 2 * pointerMetaSize > 0 {
		prevPtr, err := ptr.prev()
		if err != nil {
			return errors.Wrap(err, "failed to get freed ptr prev ptr")
		}

		if prevPtr.meta.free {
			ptr.ptr = prevPtr.ptr
			ptr.meta.size += prevPtr.meta.size + 2 * pointerMetaSize
			err := a.freelist.Delete(prevPtr.key())
			if err != nil {
				return errors.Wrap(err, "failed to delete freelist item")
			}
		}
	}

	ptr.meta.free = true
	err := a.freelist.Insert(&rbtree.Entry[*freelistKey, *rbtree.DummyVal]{
		Key: ptr.key(),
		Val: &rbtree.DummyVal{},
	})
	if err != nil {
		return errors.Wrap(err, "failed to insert ptr to freelist")
	}

	return errors.Wrap(ptr.writeMeta(), "failed to update freed ptr meta")
}

func (a *Allocator) Pointer(addr uint64, size uint32) Pointable {
	return &pointer{addr, &pointerMetadata{false, size}, a.pager}
}

func (a *Allocator) Print() error {
	fmt.Print("freelist")
	if err := a.freelist.Print(5); err != nil {
		return errors.Wrap(err, "freelist print failed")
	}

	fmt.Println("\nmeta", a.meta)
	return nil
}

func (a *Allocator) Close() error {
	if err := a.freelist.Close(); err != nil {
		return errors.Wrap(err, "failed to close free space RBT")
	}
	return errors.Wrap(a.writeMeta(), "failed to close page id RBT")
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

	ptr := a.createPointer(entry.Key.ptr + pointerMetaSize, entry.Key.size - 2 * pointerMetaSize)
	ptr.meta.free = true
	return errors.Wrap(ptr.writeMeta(), "failed to update shrinked free block meta")
}

func (a *Allocator) init() error {
	a.meta = &metadata{top: 0}
	a.metaPtr = a.createPointer(pointerMetaSize, metadataSize)

	if a.pager.Count() > 0 {
		a.meta.top += pointerMetaSize
		return a.metaPtr.Get(a.meta)
	}

	_, err := a.Alloc(metadataSize)
	return err
}

func (a *Allocator) writeMeta() error {
	return a.metaPtr.Set(a.meta)
}

func (a *Allocator) createPointer(ptr uint64, size uint32) *pointer {
	return &pointer{
		ptr:   ptr,
		meta:  &pointerMetadata{free: false, size: size},
		pager: a.pager,
	}
}

func (a *Allocator) newPointer(size uint32) *pointer {
	ptr := a.createPointer(a.meta.top + pointerMetaSize, size)
	a.meta.top += uint64(size) +  2 * pointerMetaSize
	return ptr
}
