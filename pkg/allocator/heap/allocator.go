package allocator

import (
	"fmt"
	"go-dbms/pkg/pager"
	"go-dbms/pkg/rbtree"

	"github.com/pkg/errors"
)

func Open(filename string, opts *Options) (*Allocator, error) {
	freelist, err := rbtree.Open[*pointer, *rbtree.DummyVal](
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

	a.metaPtr = a.newPointer(pointerMetaSize, metadataSize)

	return a, a.init()
}

type Allocator struct {
	freelist       *rbtree.RBTree[*pointer, *rbtree.DummyVal]
	targetPageSize uint16
	pager          *pager.Pager
	meta           *metadata
	metaPtr        *pointer
}

func (a *Allocator) Alloc(size uint32) (Pointable, error) {
	entry, err := a.freelist.Get(a.newPointer(0, size + pointerMetaSize))
	if err != nil && err != rbtree.ErrNotFound {
		return nil, errors.Wrap(err, "failed to find free space from freelist")
	} else if entry != nil {
		ptr := a.newPointer(entry.Key.ptr, size)
		entry.Key.pager = a.pager
		if err := a.shrink(entry, size + pointerMetaSize); err != nil {
			return nil, errors.Wrap(err, "failed to shrink allocated space")
		}
		return ptr, nil
	}

	requiredTotalSize := a.meta.top + uint64(size + pointerMetaSize)
	requiredExtraPages := requiredTotalSize / uint64(a.targetPageSize) + 1
	if requiredExtraPages > a.pager.Count() {
		_, err := a.pager.Alloc(int(requiredExtraPages - a.pager.Count()))
		if err != nil {
			return nil, errors.Wrap(err, "failed to alloc space")
		}
	}

	ptr := a.newPointer(a.meta.top + pointerMetaSize, size)
	meta, err := ptr.meta.MarshalBinary()
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal meta after alloc")
	}

	if err := a.pager.WriteAt(meta, a.meta.top); err != nil {
		return nil, errors.Wrap(err, "failed to write pointer meta")
	}

	a.meta.top += uint64(size + pointerMetaSize)
	return ptr, a.writeMeta()
}

func (a *Allocator) Free(p Pointable) error {
	ptr, ok := p.(*pointer)
	if !ok {
		return errors.New("invalid pointer type")
	}

	nextPtrMeta := &pointerMetadata{}
	nextPtrMetaBytes := make([]byte, pointerMetaSize)
	for ptr.ptr + uint64(ptr.meta.size) < a.meta.top {
		err := a.pager.ReadAt(nextPtrMetaBytes, ptr.ptr + uint64(ptr.meta.size))
		if err != nil {
			return errors.Wrap(err, "failed to read next pointer meta")
		}

		err = nextPtrMeta.UnmarshalBinary(nextPtrMetaBytes)
		if err != nil {
			return errors.Wrap(err, "failed to unmarshal next pointer meta")
		}
		if !nextPtrMeta.free {
			break
		}

		err = a.freelist.Delete(a.newPointer(
			uint64(nextPtrMeta.size) + pointerMetaSize,
			nextPtrMeta.size,
		))
		if err != nil {
			return errors.Wrap(err, "failed to delete ptr from freelist")
		}

		ptr.meta.size += nextPtrMeta.size + pointerMetaSize
	}

	ptr.meta.free = true
	err := a.freelist.Insert(&rbtree.Entry[*pointer, *rbtree.DummyVal]{
		Key: ptr,
		Val: &rbtree.DummyVal{},
	})
	if err != nil {
		return errors.Wrap(err, "failed to insert ptr to freelist")
	}

	return a.writeMeta()
}

func (a *Allocator) Pointer() Pointable {
	return a.newPointer(0, 0)
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
	entry *rbtree.Entry[*pointer, *rbtree.DummyVal],
	shrinkValue uint32,
) error {
	if err := a.freelist.Delete(entry.Key); err != nil {
		return errors.Wrap(err, "failed to delete free space")
	}

	remainingSpace := entry.Key.meta.size - shrinkValue
	if remainingSpace == 0 {
		return nil
	}

	entry.Key.ptr += uint64(shrinkValue)
	entry.Key.meta.size -= shrinkValue
	if err := a.freelist.Insert(entry); err != nil {
		return errors.Wrap(err, "failed to insert entry key after shrink")
	}

	return nil
}

func (a *Allocator) init() error {
	if a.pager.Count() > 0 {
		a.meta = &metadata{}
		return a.metaPtr.Get(a.meta)
	}

	a.meta = &metadata{top: 0}
	_, err := a.Alloc(metadataSize)
	return err
}

func (a *Allocator) writeMeta() error {
	return a.metaPtr.Set(a.meta)
}

func (a *Allocator) newEntry(ptr uint64, size uint32) *rbtree.Entry[*pointer, *rbtree.DummyVal] {
	return &rbtree.Entry[*pointer, *rbtree.DummyVal]{
		Key: a.newPointer(ptr, size),
		Val: &rbtree.DummyVal{},
	}
}

func (a *Allocator) newPointer(ptr uint64, size uint32) *pointer {
	return &pointer{
		ptr:   ptr,
		meta:  &pointerMetadata{free: false, size: size},
		pager: a.pager,
	}
}
