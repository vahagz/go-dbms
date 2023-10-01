package bptree

import (
	"fmt"
	"go-dbms/pkg/freelist"
)

type Freelist interface {
	Add(pageId uint64, freeSpace uint16) (freelist.PTR, error)
	AddMem(pageId uint64, freeSpace uint16) (freelist.PTR, error)
	Set(ptr freelist.PTR, freeSpace uint16) error
	SetMem(ptr freelist.PTR, freeSpace uint16) error
	Alloc(size uint16) (uint64, freelist.PTR, error)
	AllocN(n int) ([]uint64, error)
	SetRemoveFunc(fn freelist.RemoveFunc)
	SetAllocator(a freelist.Allocator)
	WriteAll() error
	Close() error
	Print() error
}

func (tree *BPlusTree) Add(pageId uint64, freeSpace uint16) (freelist.PTR, error) {
	ptr, err := tree.AddMem(pageId, freeSpace)
	if err != nil {
		return nil, err
	}
	return ptr, tree.writeAll()
}

func (tree *BPlusTree) AddMem(pageId uint64, freeSpace uint16) (freelist.PTR, error) {
	ptr := &Pointer{freeSpace, pageId}
	key, err := ptr.MarshalKey()
	if err != nil {
		return nil, err
	}

	err = tree.Put(key, nil, &PutOptions{
		Uniq:   true,
		Update: true,
	})
	if err != nil {
		return nil, err
	}

	return ptr, nil
}

func (tree *BPlusTree) Set(ptr freelist.PTR, freeSpace uint16) error {
	if err := tree.SetMem(ptr, freeSpace); err != nil {
		return err
	}

	return tree.writeAll()
}

func (tree *BPlusTree) SetMem(ptr freelist.PTR, freeSpace uint16) error {
	pt, ok := ptr.(*Pointer)
	if !ok {
		return freelist.ErrInvalidPTR
	}

	if freeSpace == 0 || (tree.removeFunc != nil && tree.removeFunc(pt.PageId, pt.FreeSpace)) {
		oldKey, err := pt.MarshalKey()
		if err != nil {
			return err
		}

		_, err = tree.Del(oldKey)
		return err
	}

	pt.FreeSpace = freeSpace
	newKey, err := pt.MarshalKey()
	if err != nil {
		return err
	}

	return tree.PutMem(newKey, nil, &PutOptions{
		Uniq:   true,
		Update: true,
	})
}

func (tree *BPlusTree) Alloc(size uint16) (uint64, freelist.PTR, error) {
	if size > tree.meta.targetPageSz {
		return 0, nil, fmt.Errorf("size must be less or equal than target page size")
	}

	ptr := &Pointer{FreeSpace: size}
	searchKey, err := ptr.MarshalKey()
	if err != nil {
		return 0, nil, err
	}

	var foundKey [][]byte
	err = tree.Scan(searchKey, false, true, func(key [][]byte, val []byte) (bool, error) {
		foundKey = key
		return true, nil
	})
	if err != nil {
		return 0, nil, err
	}

	if foundKey != nil {
		err := ptr.UnmarshalKey(foundKey)
		if err != nil {
			return 0, nil, err
		}

		ptr.FreeSpace -= size
		return ptr.PageId, ptr, tree.Set(ptr, ptr.FreeSpace)
	}

	pid, err := tree.allocator.Alloc(int(tree.meta.preAlloc))
	if err != nil {
		return 0, nil, err
	}

	err = tree.initTargetPageSeq(pid + 1, tree.meta.preAlloc - 1)
	if err != nil {
		return 0, nil, err
	}

	pt := &Pointer{
		FreeSpace: tree.meta.targetPageSz - size,
		PageId:    pid,
	}
	err = tree.Set(pt, tree.meta.targetPageSz - size)
	if err != nil {
		return 0, nil, err
	}

	return pid, pt, tree.writeAll()
}

func (tree *BPlusTree) AllocN(n int) ([]uint64, error) {
	if n <= 0 {
		return nil, fmt.Errorf("invalid allocN size")
	}

	ptr := &Pointer{
		FreeSpace: uint16(tree.meta.targetPageSz),
	}
	key, err := ptr.MarshalKey()
	if err != nil {
		return nil, err
	}

	result := []uint64{}
	tree.Scan(key, false, true, func(key [][]byte, val []byte) (bool, error) {
		if err := ptr.UnmarshalKey(key); err != nil {
			return false, err
		}

		result = append(result, ptr.PageId)
		n--
		return n == 0, nil
	})

	if len(result) == n {
		return result, nil
	}

	pid, err := tree.allocator.Alloc(n)
	if err != nil {
		return nil, nil
	}

	for i := 0; i < n; i++ {
		result = append(result, pid + uint64(i))
	}
	return result, nil
}

func (tree *BPlusTree) SetRemoveFunc(fn freelist.RemoveFunc) {
	tree.removeFunc = fn
}

func (tree *BPlusTree) SetAllocator(a freelist.Allocator) {
	tree.allocator = a
}

func (tree *BPlusTree) WriteAll() error {
	return tree.writeAll()
}

func (tree *BPlusTree) Print() error {
	if err := tree.freelist.Print(); err != nil {
		return err
	}

	return tree.Scan(nil, false, true, func(key [][]byte, val []byte) (bool, error) {
		ptr := &Pointer{}
		err := ptr.UnmarshalKey(key)
		if err != nil {
			return false, err
		}

		fmt.Println(ptr)
		return false, nil
	})
}

func (tree *BPlusTree) initTargetPageSeq(startPid uint64, count uint16) error {
	for i := 0; i < int(count); i++ {
		err := tree.SetMem(&Pointer{
			FreeSpace: tree.meta.targetPageSz,
			PageId:    startPid + uint64(i),
		}, tree.meta.targetPageSz)
		if err != nil {
			return err
		}
	}
	return nil
}
