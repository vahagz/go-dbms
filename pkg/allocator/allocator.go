package freelist

import (
	"fmt"
	"go-dbms/pkg/rbtree"

	"github.com/pkg/errors"
)

func Open(filename string, opts *Options) (*Allocator, error) {
	RBTopts := &rbtree.Options{
		PageSize: opts.TreePageSize,
		KeySize:  itemSize,
	}

	RBTopts.ValSize = 8
	freeSpaceRBT, err := rbtree.Open[*Item[uint16], *Item[uint64]](
		fmt.Sprintf("%s_freespace.bin", filename),
		RBTopts,
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open rbtree for allocator")
	}

	RBTopts.ValSize = 2
	pageIdRBT, err := rbtree.Open[*Item[uint64], *Item[uint16]](
		fmt.Sprintf("%s_pageid.bin", filename),
		RBTopts,
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open rbtree for allocator")
	}

	return &Allocator{
		freeSpaceRBT:   freeSpaceRBT,
		pageIdRBT:      pageIdRBT,
		targetPageSize: opts.TargetPageSize,
		pager:          opts.Pager,
		removeFunc:     opts.RemoveFunc,
	}, nil
}

type Pager interface {
	Alloc(size int) (uint64, error)
}

type RemoveFunc func(pageId uint64, freeSpace uint16) bool

type Allocator struct {
	freeSpaceRBT   *rbtree.RBTree[*Item[uint16], *Item[uint64]]
	pageIdRBT      *rbtree.RBTree[*Item[uint64], *Item[uint16]]
	targetPageSize uint16
	pager          Pager
	removeFunc     RemoveFunc
}

func (a *Allocator) Alloc(size int) (uint64, error) {
	if size > int(a.targetPageSize) {
		return 0, errors.New("can't allocate space bigger than page size")
	}

	entry, err := a.freeSpaceRBT.Get(newKey(uint16(size)))
	if err != nil && err != rbtree.ErrNotFound {
		return 0, errors.Wrap(err, "failed to find free space from freelist")
	} else if entry != nil {
		if err := a.shrink(entry, uint16(size)); err != nil {
			return 0, errors.Wrap(err, "failed to shrink allocated space")
		}
		return entry.Val.Val, nil
	}

	pageId, err := a.pager.Alloc(1)
	if err != nil {
		return 0, errors.Wrap(err, "failed to alloc page")
	}

	entry = newEntry(uint16(a.targetPageSize) - uint16(size), pageId)
	if err := a.freeSpaceRBT.Insert(entry); err != nil {
		return 0, errors.Wrap(err, "failed to insert free space")
	}
	if err := a.pageIdRBT.Insert(a.swapEntry(entry)); err != nil {
		return 0, errors.Wrap(err, "failed to insert free space [page id]")
	}

	return pageId, nil
}

func (a *Allocator) swapEntry(
	entry *rbtree.Entry[*Item[uint16], *Item[uint64]],
) *rbtree.Entry[*Item[uint64], *Item[uint16]] {
	return &rbtree.Entry[*Item[uint64], *Item[uint16]]{
		Key: entry.Val,
		Val: entry.Key,
	}
}

func (a *Allocator) shrink(
	entry *rbtree.Entry[*Item[uint16], *Item[uint64]],
	shrinkValue uint16,
) error {
	err := a.shrinkFreeSpace(entry, uint16(shrinkValue))
	if err != nil {
		return errors.Wrap(err, "failed to shrink free space")
	}

	err = a.shrinkPageId(a.swapEntry(entry), uint16(shrinkValue))
	if err != nil {
		return errors.Wrap(err, "failed to shrink page id")
	}

	return nil
}

func (a *Allocator) shrinkFreeSpace(
	entry *rbtree.Entry[*Item[uint16], *Item[uint64]],
	shrinkValue uint16,
) error {
	if err := a.freeSpaceRBT.Delete(entry.Key); err != nil {
		return errors.Wrap(err, "failed to delete entry key to shrink")
	}

	entry.Key.Val -= shrinkValue
	if err := a.freeSpaceRBT.Insert(entry); err != nil {
		return errors.Wrap(err, "failed to insert entry key after shrink")
	}

	return nil
}

func (a *Allocator) shrinkPageId(
	entry *rbtree.Entry[*Item[uint64], *Item[uint16]],
	shrinkValue uint16,
) error {
	if err := a.pageIdRBT.Delete(entry.Key); err != nil {
		return errors.Wrap(err, "failed to delete entry key to shrink")
	}

	entry.Val.Val -= shrinkValue
	if err := a.pageIdRBT.Insert(entry); err != nil {
		return errors.Wrap(err, "failed to insert entry key after shrink")
	}

	return nil
}
