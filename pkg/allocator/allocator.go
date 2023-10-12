package allocator

import (
	"fmt"
	"go-dbms/pkg/rbtree"

	"github.com/pkg/errors"
)

func Open(filename string, opts *Options) (*Allocator, error) {
	RBTopts := &rbtree.Options{
		PageSize: opts.TreePageSize,
	}

	freeSpaceRBT, err := rbtree.Open[*ItemFP, *rbtree.DummyVal](
		fmt.Sprintf("%s_freespace.bin", filename),
		RBTopts,
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open rbtree for allocator")
	}

	pageIdRBT, err := rbtree.Open[*ItemPF, *rbtree.DummyVal](
		fmt.Sprintf("%s_pageid.bin", filename),
		RBTopts,
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open rbtree for allocator")
	}

	return &Allocator{
		freeSpaceRBT:         freeSpaceRBT,
		pageIdRBT:            pageIdRBT,
		targetPageSize:       opts.TargetPageSize,
		targetPageHeaderSize: opts.TargetPageHeaderSize,
		pager:                opts.Pager,
		removeFunc:           opts.RemoveFunc,
	}, nil
}

type Pager interface {
	Alloc(size int) (uint64, error)
}

type RemoveFunc func(pageId uint64, freeSpace uint16) bool

type Allocator struct {
	freeSpaceRBT         *rbtree.RBTree[*ItemFP, *rbtree.DummyVal]
	pageIdRBT            *rbtree.RBTree[*ItemPF, *rbtree.DummyVal]
	targetPageSize       uint16
	targetPageHeaderSize uint16
	pager                Pager
	removeFunc           RemoveFunc
}

func (a *Allocator) Alloc(size uint16) (uint64, error) {
	if size > a.targetPageSize - a.targetPageHeaderSize {
		return 0, errors.New("can't allocate space bigger than page size")
	}

	entry, err := a.freeSpaceRBT.Get(newKey[*ItemFP](size, 0))
	if err != nil && err != rbtree.ErrNotFound {
		return 0, errors.Wrap(err, "failed to find free space from freelist")
	} else if entry != nil {
		if err := a.shrink(entry, size); err != nil {
			return 0, errors.Wrap(err, "failed to shrink allocated space")
		}
		return entry.Key.PageId, nil
	}

	pageId, err := a.pager.Alloc(1)
	if err != nil {
		return 0, errors.Wrap(err, "failed to alloc page")
	}

	remainingSpace := a.targetPageSize - a.targetPageHeaderSize - size
	if remainingSpace > 0 && (a.removeFunc == nil || !a.removeFunc(pageId, remainingSpace)) {
		entry = newEntry[*ItemFP](remainingSpace, pageId)
		if err := a.freeSpaceRBT.Insert(entry); err != nil {
			return 0, errors.Wrap(err, "failed to insert free space")
		}
		if err := a.pageIdRBT.Insert(swap[*ItemPF](entry)); err != nil {
			return 0, errors.Wrap(err, "failed to insert free space [page id]")
		}
	}

	return pageId, nil
}

func (a *Allocator) Free(pageId uint64, size uint16) error {
	if size > a.targetPageSize - a.targetPageHeaderSize {
		return errors.New("can't free space bigger than page size")
	}

	entry, err := a.pageIdRBT.Get(newKey[*ItemPF](0, pageId))
	if err != nil && err != rbtree.ErrNotFound {
		return errors.Wrap(err, "failed to find page free space from freelist")
	} else if entry != nil {
		if entry.Key.FreeSpace + size > a.targetPageSize - a.targetPageHeaderSize {
			return errors.New("invalid free size")
		}

		if err := a.extend(entry, size); err != nil {
			return errors.Wrap(err, "failed to extend freed space")
		}
		return nil
	}
	
	entry = newEntry[*ItemPF](size, pageId)
	if err := a.freeSpaceRBT.Insert(swap[*ItemFP](entry)); err != nil {
		return errors.Wrap(err, "failed to insert free space")
	}
	if err := a.pageIdRBT.Insert(entry); err != nil {
		return errors.Wrap(err, "failed to insert free space [page id]")
	}
	return nil
}

func (a *Allocator) Print() error {
	fmt.Println("freeSpaceRBT")
	if err := a.freeSpaceRBT.Print(5); err != nil {
		return errors.Wrap(err, "freeSpaceRBT print failed")
	}
	fmt.Println("pageIdRBT")
	if err := a.pageIdRBT.Print(5); err != nil {
		return errors.Wrap(err, "pageIdRBT print failed")
	}
	return nil
}

func (a *Allocator) Close() error {
	if err := a.freeSpaceRBT.Close(); err != nil {
		return errors.Wrap(err, "failed to close free space RBT")
	}
	return errors.Wrap(a.pageIdRBT.Close(), "failed to close page id RBT")
}

func (a *Allocator) extend(
	entry *rbtree.Entry[*ItemPF, *rbtree.DummyVal],
	extendValue uint16,
) error {
	err := a.extendFreeSpace(swap[*ItemFP](entry), extendValue)
	if err != nil {
		return errors.Wrap(err, "failed to extend free space")
	}

	err = a.extendPageId(entry, extendValue)
	if err != nil {
		return errors.Wrap(err, "failed to extend page id")
	}

	return nil
}

func (a *Allocator) extendFreeSpace(
	entry *rbtree.Entry[*ItemFP, *rbtree.DummyVal],
	extendValue uint16,
) error {
	if err := a.freeSpaceRBT.Delete(entry.Key); err != nil {
		return errors.Wrap(err, "failed to delete entry key to extend")
	}

	entry.Key.FreeSpace += extendValue
	if err := a.freeSpaceRBT.Insert(entry); err != nil {
		return errors.Wrap(err, "failed to insert entry key after extend")
	}
	entry.Key.FreeSpace -= extendValue

	return nil
}

func (a *Allocator) extendPageId(
	entry *rbtree.Entry[*ItemPF, *rbtree.DummyVal],
	extendValue uint16,
) error {
	if err := a.pageIdRBT.Delete(entry.Key); err != nil {
		return errors.Wrap(err, "failed to delete entry key to extend")
	}

	entry.Key.FreeSpace += extendValue
	if err := a.pageIdRBT.Insert(entry); err != nil {
		return errors.Wrap(err, "failed to insert entry key after extend")
	}
	entry.Key.FreeSpace -= extendValue

	return nil
}

func (a *Allocator) shrink(
	entry *rbtree.Entry[*ItemFP, *rbtree.DummyVal],
	shrinkValue uint16,
) error {
	remainingSpace := entry.Key.FreeSpace - shrinkValue
	if remainingSpace == 0 || (a.removeFunc != nil && a.removeFunc(entry.Key.PageId, remainingSpace)) {
		if err := a.freeSpaceRBT.Delete(entry.Key); err != nil {
			return errors.Wrap(err, "failed to delete free space")
		}
		if err := a.pageIdRBT.Delete(swap[*ItemPF](entry).Key); err != nil {
			return errors.Wrap(err, "failed to delete free space")
		}
		return nil
	}

	err := a.shrinkFreeSpace(entry, shrinkValue)
	if err != nil {
		return errors.Wrap(err, "failed to shrink free space")
	}

	err = a.shrinkPageId(swap[*ItemPF](entry), shrinkValue)
	if err != nil {
		return errors.Wrap(err, "failed to shrink page id")
	}

	return nil
}

func (a *Allocator) shrinkFreeSpace(
	entry *rbtree.Entry[*ItemFP, *rbtree.DummyVal],
	shrinkValue uint16,
) error {
	if err := a.freeSpaceRBT.Delete(entry.Key); err != nil {
		return errors.Wrap(err, "failed to delete entry key to shrink")
	}

	entry.Key.FreeSpace -= shrinkValue
	if err := a.freeSpaceRBT.Insert(entry); err != nil {
		return errors.Wrap(err, "failed to insert entry key after shrink")
	}
	entry.Key.FreeSpace += shrinkValue

	return nil
}

func (a *Allocator) shrinkPageId(
	entry *rbtree.Entry[*ItemPF, *rbtree.DummyVal],
	shrinkValue uint16,
) error {
	if err := a.pageIdRBT.Delete(entry.Key); err != nil {
		return errors.Wrap(err, "failed to delete entry key to shrink")
	}

	entry.Key.FreeSpace -= shrinkValue
	if err := a.pageIdRBT.Insert(entry); err != nil {
		return errors.Wrap(err, "failed to insert entry key after shrink")
	}
	entry.Key.FreeSpace += shrinkValue

	return nil
}
