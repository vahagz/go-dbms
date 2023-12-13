// Package bptree implements an on-disk B+ tree indexing scheme that can store
// key-value pairs and provide fast lookups and range scans. keys and values
// can be blobs binary data.
package bptree

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"sync"

	allocator "go-dbms/pkg/allocator/heap"
	"go-dbms/pkg/cache"
	"go-dbms/pkg/customerrors"
	"go-dbms/pkg/pager"
	"go-dbms/util/helpers"

	"github.com/pkg/errors"
)

// bin is the byte order used for all marshals/unmarshals.
var bin = binary.LittleEndian

// these values used to set extra counter value
// FILL will set all bits with 1
// ZERO will set all bits with 0
// CURRENT will set current counter value (tree.meta.counter)

type counterOption int

const (
	counterFill counterOption = iota
	counterZero
	counterCurrent
)

type nodeType int

const (
	nodeLeaf nodeType = iota
	nodeInternal
)

// Open opens the named file as a B+ tree index file and returns an instance
// B+ tree for use. Use ":memory:" for an in-memory B+ tree instance for quick
// testing setup. Degree of the tree is computed based on maxKeySize and pageSize
// used by the pager. If nil options are provided, defaultOptions will be used.
func Open(fileName string, opts *Options) (*BPlusTree, error) {
	if opts == nil {
		opts = &defaultOptions
	}

	p, err := pager.Open(fmt.Sprintf("%s.idx", fileName), int(opts.PageSize), false, 0644)
	if err != nil {
		return nil, err
	}
	
	heap, err := allocator.Open(fileName, &allocator.Options{
		TargetPageSize: uint16(opts.PageSize),
		TreePageSize:   uint16(opts.PageSize),
		Pager:          p,
	})
	if err != nil {
		return nil, err
	}

	tree := &BPlusTree{
		file:   fileName,
		mu:     &sync.RWMutex{},
		heap:   heap,
	}

	tree.cache = cache.NewCache[*node](10000, tree.newNode)

	if err := tree.open(opts); err != nil {
		_ = tree.Close()
		return nil, err
	}

	return tree, nil
}

// BPlusTree represents an on-disk B+ tree. Each node in the tree is mapped
// to a single page in the file. Degree of the tree is decided based on the
// page size and max key size while initializing.
type BPlusTree struct {
	file    string
	metaPtr allocator.Pointable

	// tree state
	mu    *sync.RWMutex
	heap  *allocator.Allocator
	cache *cache.Cache[*node]
	meta  *metadata
}

// Get fetches the value associated with the given key.
// Returns error if key not found.
func (tree *BPlusTree) Get(key [][]byte) ([][]byte, error) {
	if key == nil || len(key) == 0 {
		return nil, customerrors.ErrEmptyKey
	}

	result := [][]byte{}
	return result, tree.Scan(key, ScanOptions{
		Reverse: false,
		Strict:  true,
	}, func(k [][]byte, v []byte) (bool, error) {
		if helpers.CompareMatrix(key, tree.removeCounterIfRequired(k)) != 0 {
			return true, nil
		}

		result = append(result, v)
		return false, nil
	})
}

// Put puts the key-value pair into the B+ tree. If the key already exists,
// its value will be updated.
func (tree *BPlusTree) Put(key [][]byte, val []byte, opt PutOptions) (bool, error) {
	success, err := tree.PutMem(key, val, opt)
	if err != nil {
		return false, err
	}

	return success, tree.WriteAll()
}

func (tree *BPlusTree) PutMem(key [][]byte, val []byte, opt PutOptions) (bool, error) {
	key = helpers.Copy(key)
	keylen := 0
	for _, v := range key {
		keylen += len(v)
	}

	if keylen > int(tree.meta.keySize) {
		return false, customerrors.ErrKeyTooLarge
	} else if keylen == 0 {
		return false, customerrors.ErrEmptyKey
	}

	tree.mu.Lock()
	defer tree.mu.Unlock()

	e := entry{
		key: key,
		val: val,
	}

	success, err := tree.put(e, opt)
	if err != nil {
		return false, err
	}

	if success && !opt.Update {
		tree.meta.counter++
		tree.meta.size++
		tree.meta.dirty = true
	}

	return success, nil
}

// Del removes the key-value entry from the B+ tree. If the key does not
// exist, returns error.
func (tree *BPlusTree) Del(key [][]byte) (int, error) {
	return tree.DelMem(key), tree.WriteAll()
}

func (tree *BPlusTree) DelMem(key [][]byte) int {
	tree.mu.Lock()
	defer tree.mu.Unlock()

	key = tree.addCounterIfRequired(helpers.Copy(key), counterZero)
	count := 0

	cnt := true
	for cnt {
		cnt = false
		tree.scan(key, ScanOptions{
			Reverse: false,
			Strict:  true,
		}, cache.NONE, func(
			k [][]byte,
			_ []byte,
			_ int,
			ptr cache.Pointable[*node],
		) (bool, error) {
			if helpers.CompareMatrix(
				tree.removeCounterIfRequired(key),
				tree.removeCounterIfRequired(k),
			) == 0 {
				cnt = true
				ptr.Lock()
				if isDelete := tree.del(k, ptr); isDelete {
					count++
				}
			}
			return true, nil
		})
	}

	if count > 0 {
		tree.meta.size -= uint32(count)
		tree.meta.dirty = true
	}

	return count
}

// Scan performs an index scan starting at the given key. Each entry will be
// passed to the scanFn. If the key is zero valued (nil or len=0), then the
// left/right leaf key will be used as the starting key. Scan continues until
// the right most leaf node is reached or the scanFn returns 'true' indicating
// to stop the scan. If reverse=true, scan starts at the right most node and
// executes in descending order of keys.
func (tree *BPlusTree) Scan(
	key [][]byte,
	opts ScanOptions,
	scanFn func(key [][]byte, val []byte) (bool, error),
) error {
	tree.mu.RLock()
	defer tree.mu.RUnlock()

	if tree.meta.size == 0 {
		return nil
	}
	
	if len(key) != 0 {
		// we have a specific key to start at. find the node containing the
		// key and start the scan there.
		key = helpers.Copy(key)
		if (opts.Strict && opts.Reverse) || (!opts.Strict && !opts.Reverse) {
			key = tree.addCounterIfRequired(key, counterFill)
		} else {
			key = tree.addCounterIfRequired(key, counterZero)
		}
	}

	return tree.scan(key, opts, cache.READ, func(
		key [][]byte,
		val []byte,
		_ int,
		_ cache.Pointable[*node],
	) (bool, error) {
		return scanFn(key, val)
	})
}

func (tree *BPlusTree) PrepareSpace(size uint32) {
	tree.heap.PreAlloc(size)
}

func (tree *BPlusTree) Count() (int, error) {
	counter := 0
	err := tree.Scan(nil, ScanOptions{
		Reverse: false,
		Strict:  true,
	}, func(_ [][]byte, _ []byte) (bool, error) {
		counter++
		return false, nil
	})
	return counter, err
}

// Size returns the number of entries in the entire tree
func (tree *BPlusTree) Size() int64 { return int64(tree.meta.size) }

func (tree *BPlusTree) IsUniq() bool { return helpers.GetBit(tree.meta.flags, uniquenessBit) }

// Close flushes any writes and closes the underlying pager.
func (tree *BPlusTree) Close() error {
	tree.mu.Lock()
	defer tree.mu.Unlock()

	if tree.heap == nil {
		return nil
	}

	_ = tree.WriteAll() // write if any nodes are pending
	err := tree.heap.Close()
	tree.heap = nil
	return err
}

func (tree *BPlusTree) String() string {
	return fmt.Sprintf(
		"BPlusTree{file='%s', size=%d, degree=%d}",
		tree.file, tree.Size(), tree.meta.degree,
	)
}

func (tree *BPlusTree) Print() {
	root := tree.rootR()
	defer root.RUnlock()
	fmt.Println("============= bptree =============")
	tree.print(root, 0, cache.READ)
	fmt.Println("============ freelist ============")
	tree.heap.Print()
	fmt.Println("==================================")
}

func (tree *BPlusTree) ClearCache() {
	tree.cache.Clear()
}

func (tree *BPlusTree) CheckConsistency(list [][]byte) bool {
	maxChildCount := tree.meta.degree
	maxEntryCount := maxChildCount - 1
	minChildCount := uint16(math.Ceil(float64(tree.meta.degree) / 2))
	minEntryCount := minChildCount - 1

	type Itm struct{
		val   []byte
		count int
	}

	m := map[string]*Itm{}
	for i := range list {
		if itm, ok := m[string(list[i])]; ok {
			itm.count++
		} else {
			m[string(list[i])] = &Itm{
				val: list[i],
				count: 1,
			}
		}
	}

	defer func() {
		if err := recover(); err != nil {
			if v, check := err.(bool); check && v == false {
				return
			}

			panic(err)
		}
	}()

	var traverse func(nPtr cache.Pointable[*node], indent int, flag cache.LOCKMODE)
	traverse = func(nPtr cache.Pointable[*node], indent int, flag cache.LOCKMODE) {
		n := nPtr.Get()

		entryCount := uint16(len(n.entries))
		childCount := uint16(len(n.children))
		if (
				nPtr.Ptr().Addr() == tree.meta.root.Addr() && (
					entryCount == 0 ||
					entryCount > maxEntryCount)) || (
				nPtr.Ptr().Addr() != tree.meta.root.Addr() && (
					entryCount < minEntryCount ||
					entryCount > maxEntryCount)) {
			if !n.isLeaf() && (
				childCount < minChildCount ||
				childCount > maxChildCount) {
					fmt.Println("node violated")
					fmt.Println("entry count =>", len(n.entries))
					fmt.Println("child count =>", len(n.children))
					fmt.Println("ptr =>", nPtr.Ptr().Addr())
					panic(false)
				}
		}

		for i := len(n.entries) - 1; i >= 0; i-- {
			if !n.isLeaf() {
				child := tree.fetchF(n.children[i+1], flag)
				traverse(child, indent + 4, flag)
				defer child.UnlockFlag(flag)
			}
			
			if _, ex := m[string(n.entries[i].key[0])]; !ex {
				if n.isLeaf() {
					fmt.Println("unexpected leaf entry value =>", n.entries[i].key[0])
				} else if !n.isLeaf() {
					fmt.Println("unexpected internal entry value =>", n.entries[i].key[0])
				}
				panic(false)
			}
		}

		if !n.isLeaf() {
			child := tree.fetchF(n.children[0], flag)
			traverse(child, indent + 4, flag)
			defer child.UnlockFlag(flag)
		}
	}

	traverse(tree.rootF(cache.NONE), 0, cache.NONE)

	for k := range m {
		vals, err := tree.Get([][]byte{m[k].val})
		if err != nil {
			panic(err)
		} else if len(vals) == 0 {
			fmt.Println("key not found =>", m[k])
			panic(false)
		}
	}

	return true
}

func (tree *BPlusTree) print(nPtr cache.Pointable[*node], indent int, flag cache.LOCKMODE) {
	n := nPtr.Get()
	for i := len(n.entries) - 1; i >= 0; i-- {
		if !n.isLeaf() {
			child := tree.fetchF(n.children[i+1], flag)
			tree.print(child, indent + 4, flag)
			defer child.UnlockFlag(flag)
		}
		// binary.BigEndian.Uint32(n.entries[i].key[0])
		fmt.Printf("%*s%v(%v)\n", indent, "", n.entries[i].key, nPtr.Ptr().Addr())
	}

	if !n.isLeaf() {
		child := tree.fetchF(n.children[0], flag)
		tree.print(child, indent + 4, flag)
		defer child.UnlockFlag(flag)
	}
}

func (tree *BPlusTree) scan(
	key [][]byte,
	opts ScanOptions,
	flag cache.LOCKMODE,
	scanFn func(key [][]byte, val []byte, index int, leaf cache.Pointable[*node]) (bool, error),
) error {
	var beginAt cache.Pointable[*node]
	idx := 0

	root := tree.rootF(flag)
	if len(key) == 0 {
		// No explicit key provided by user, find the a leaf-node based on
		// scan direction and start there.
		if !opts.Reverse {
			beginAt = tree.leftLeaf(root, flag)
			idx = 0
		} else {
			beginAt = tree.rightLeaf(root, flag)
			idx = len(beginAt.Get().entries) - 1
		}
	} else {
		beginAt, idx, _ = tree.searchRec(root, key, flag)
		if opts.Reverse {
			idx--
		}
	}

	// starting at found leaf node, follow the 'next' pointer until.
	var nextNode allocator.Pointable

	L: for beginAt != nil {
		if !opts.Reverse {
			for i := idx; i < len(beginAt.Get().entries); i++ {
				e := beginAt.Get().entries[i]
				if stop, err := scanFn(e.key, e.val, i, beginAt); err != nil {
					beginAt.UnlockFlag(flag)
					return err
				} else if stop {
					beginAt.UnlockFlag(flag)
					break L
				}
			}
			nextNode = beginAt.Get().right
		} else {
			for i := idx; i >= 0; i-- {
				e := beginAt.Get().entries[i]
				if stop, err := scanFn(e.key, e.val, i, beginAt); err != nil {
					beginAt.UnlockFlag(flag)
					return err
				} else if stop {
					beginAt.UnlockFlag(flag)
					break L
				}
			}
			nextNode = beginAt.Get().left
		}

		beginAt.UnlockFlag(flag)
		if nextNode.IsNil() {
			break
		}

		beginAt = tree.fetchF(nextNode, flag)
		if !opts.Reverse {
			idx = 0
		} else {
			idx = len(beginAt.Get().entries) - 1
		}
	}

	return nil
}

func (tree *BPlusTree) insert(e entry) error {
	root := tree.rootW()
	leaf, index, found := tree.searchRec(root, e.key, cache.WRITE)
	if found && tree.IsUniq() {
		return errors.New("key already exists")
	}

	leaf.Get().insertEntry(index, e)
	if leaf.Get().IsFull() {
		tree.split(leaf)
		return nil
	}

	leaf.Unlock()
	return nil
}

func (tree *BPlusTree) update(e entry) (updated bool, err error) {
	return updated, tree.scan(e.key, ScanOptions{
		Reverse: false,
		Strict:  true,
	}, cache.WRITE, func(
		key [][]byte,
		val []byte,
		index int,
		leaf cache.Pointable[*node],
	) (bool, error) {
		if helpers.CompareMatrix(e.key, key) == 0 && bytes.Compare(e.val, val) != 0 {
			leaf.Get().update(index, e.val)
			updated = true
		}
		return true, nil
	})
}

// always returns true is Update is false in PutOptions
// otherwise returns true if found entry that should be updated
func (tree *BPlusTree) put(e entry, opt PutOptions) (bool, error) {
	if opt.Update {
		return tree.update(e)
	}
	e.key = tree.addCounterIfRequired(e.key, counterCurrent)
	return true, tree.insert(e)
}

func (tree *BPlusTree) split(nPtr cache.Pointable[*node]) {
	nv := nPtr.Get()
	var siblingPtr cache.Pointable[*node]
	if nv.isLeaf() {
		siblingPtr = tree.alloc(nodeLeaf)
	} else {
		siblingPtr = tree.alloc(nodeInternal)
	}

	sv := siblingPtr.Get()
	breakPoint := int(math.Ceil(float64(tree.meta.degree-1) / 2))
	pe := nv.entries[breakPoint]

	nv.Dirty(true)
	sv.Dirty(true)

	sv.parent = nv.parent
	if nv.isLeaf() {
		sv.entries = make([]entry, 0, tree.meta.degree)
		sv.entries = append(sv.entries, nv.entries[breakPoint:]...)
		nv.entries = nv.entries[:breakPoint]

		pe.val = nil

		sv.right = nv.right
		sv.left = nPtr.Ptr()
		nv.right = siblingPtr.Ptr()
		if !sv.right.IsNil() {
			nNext := tree.fetchW(sv.right)
			nNext.Get().Dirty(true)
			nNext.Get().left = siblingPtr.Ptr()
			nNext.Unlock()
		}
	} else {
		sv.entries = make([]entry, 0, tree.meta.degree)
		sv.entries = append(sv.entries, nv.entries[breakPoint+1:]...)
		sv.children = make([]allocator.Pointable, 0, tree.meta.degree+1)
		sv.children = append(sv.children, nv.children[breakPoint+1:]...)
		for _, sChildPtr := range sv.children {
			sChild := tree.fetchW(sChildPtr)
			scv := sChild.Get()
			scv.Dirty(true)
			scv.parent = siblingPtr.Ptr()
			sChild.Unlock()
		}

		nv.entries = nv.entries[:breakPoint]
		nv.children = nv.children[:breakPoint+1]
	}

	var pPtr cache.Pointable[*node]
	if nv.parent.IsNil() {
		pPtr = tree.alloc(nodeInternal)
		tree.meta.dirty = true
		tree.meta.root = pPtr.Ptr()
		sv.parent = tree.meta.root
		nv.parent = tree.meta.root
		pPtr.Get().insertChild(0, nPtr.Ptr())
	} else {
		pPtr = tree.fetchW(nv.parent)
	}

	pv := pPtr.Get()
	pv.Dirty(true)
	index, _ := pv.search(pe.key)
	pv.insertEntry(index, pe)
	pv.insertChild(index + 1, siblingPtr.Ptr())

	if pv.parent.IsNil() {
		tree.meta.dirty = true
		tree.meta.root = pPtr.Ptr()
	}

	nPtr.Unlock()
	siblingPtr.Unlock()
	if pv.IsFull() {
		tree.split(pPtr)
		return
	}
	pPtr.Unlock()
}

func (tree *BPlusTree) removeFromLeaf(key [][]byte, pPtr, nPtr cache.Pointable[*node]) {
	nv := nPtr.Get()
	index, found := nv.search(key)
	if !found {
		panic(errors.New("[removeFromLeaf] key not found"))
	}

	nv.removeEntries(index, index + 1)
	if pPtr != nil {
		pv := pPtr.Get()
		index, _ := pv.search(key)
		if index != 0 && len(nv.entries) > 0 {
			pv.Dirty(true)
			pv.entries[index - 1] = entry{
				key: helpers.Copy(nv.entries[0].key),
			}
		}
	}
}


func (tree *BPlusTree) removeFromInternal(key [][]byte, nPtr cache.Pointable[*node]) {
	nv := nPtr.Get()
	index, found := nv.search(key)
	if found {
		leftMostLeaf := tree.fetchR(nv.children[index])
		leftMostLeaf = tree.leftLeaf(leftMostLeaf, cache.READ)
		defer leftMostLeaf.RUnlock()

		lv := leftMostLeaf.Get()
		nv.Dirty(true)
		nv.entries[index - 1] = entry{
			key: helpers.Copy(lv.entries[0].key),
		}
	}
}

func (tree *BPlusTree) borrowKeyFromRightLeaf(pPtr, nPtr, rightPtr cache.Pointable[*node]) {
	nv := nPtr.Get()
	rv := rightPtr.Get()
	nv.appendEntry(rv.entries[0])
	rv.removeEntries(0, 1)

	pv := pPtr.Get()
	index, _ := pv.search(rv.entries[len(rv.entries)-1].key)
	pv.Dirty(true)
	pv.entries[index - 1] = entry{
		key: helpers.Copy(rv.entries[0].key),
	}
}

func (tree *BPlusTree) borrowKeyFromLeftLeaf(pPtr, nPtr, leftPtr cache.Pointable[*node]) {
	nv := nPtr.Get()
	lv := leftPtr.Get()
	nv.insertEntry(0, lv.entries[len(lv.entries)-1])
	lv.removeEntries(len(lv.entries) - 1, len(lv.entries))

	pv := pPtr.Get()
	index, _ := pv.search(nv.entries[len(nv.entries)-1].key)
	pv.Dirty(true)
	pv.entries[index - 1] = entry{
		key: helpers.Copy(nv.entries[0].key),
	}
}

func (tree *BPlusTree) mergeNodeWithRightLeaf(pPtr, nPtr, rightPtr cache.Pointable[*node]) {
	nv := nPtr.Get()
	rv := rightPtr.Get()
	nv.Dirty(true)
	rv.Dirty(true)

	nv.entries = append(nv.entries, rv.entries...)
	nv.right = rv.right
	if !nv.right.IsNil() {
		rightRightPtr := tree.fetchW(nv.right)
		defer rightRightPtr.Unlock()

		rrv := rightRightPtr.Get()
		rrv.left = nPtr.Ptr()
	}

	pv := pPtr.Get()
	index, _ := pv.search(rv.entries[len(rv.entries)-1].key)
	pv.removeEntries(index - 1, index)
	pv.removeChildren(index, index + 1)

	tree.freeNode(rightPtr)
}

func (tree *BPlusTree) mergeNodeWithLeftLeaf(pPtr, nPtr, leftPtr, rightPtr cache.Pointable[*node]) {
	nv := nPtr.Get()
	lv := leftPtr.Get()
	nv.Dirty(true)
	lv.Dirty(true)

	lv.entries = append(lv.entries, nv.entries...)
	lv.right = nv.right
	if !lv.right.IsNil() {
		rv := rightPtr.Get()
		rv.Dirty(true)
		rv.left = leftPtr.Ptr()
	}

	pv := pPtr.Get()
	index, _ := pv.search(nv.entries[len(nv.entries)-1].key)
	pv.removeEntries(index - 1, index)
	pv.removeChildren(index, index + 1)

	tree.freeNode(nPtr)
}

func (tree *BPlusTree) borrowKeyFromRightInternal(parentIndex int, pPtr, nPtr, rightPtr cache.Pointable[*node]) {
	pv := pPtr.Get()
	nv := nPtr.Get()
	rv := rightPtr.Get()

	nv.appendEntry(pv.entries[parentIndex])
	pv.Dirty(true)
	pv.entries[parentIndex] = entry{
		key: helpers.Copy(rv.entries[0].key),
	}
	rv.removeEntries(0, 1)
	nv.appendChild(rv.children[0])
	rv.removeChildren(0, 1)
	
	lastChildPtr := tree.fetchW(nv.children[len(nv.children)-1])
	defer lastChildPtr.Unlock()
	lcv := lastChildPtr.Get()
	lcv.Dirty(true)
	lcv.parent = nPtr.Ptr()
}

func (tree *BPlusTree) borrowKeyFromLeftInternal(parentIndex int, pPtr, nPtr, leftPtr cache.Pointable[*node]) {
	pv := pPtr.Get()
	nv := nPtr.Get()
	lv := leftPtr.Get()

	nv.insertEntry(0, pv.entries[parentIndex - 1])
	pv.Dirty(true)
	pv.entries[parentIndex - 1] = entry{
		key: helpers.Copy(lv.entries[len(lv.entries)-1].key),
	}
	lv.removeEntries(len(lv.entries)-1, len(lv.entries))
	nv.insertChild(0, lv.children[len(lv.children)-1])
	lv.removeChildren(len(lv.children) - 1, len(lv.children))

	firstChildPtr := tree.fetchW(nv.children[0])
	defer firstChildPtr.Unlock()
	fcv := firstChildPtr.Get()
	fcv.Dirty(true)
	fcv.parent = nPtr.Ptr()
}

func (tree *BPlusTree) mergeNodeWithRightInternal(parentIndex int, pPtr, nPtr, rightPtr cache.Pointable[*node]) {
	pv := pPtr.Get()
	nv := nPtr.Get()
	rv := rightPtr.Get()

	nv.appendEntry(pv.entries[parentIndex])
	pv.removeEntries(parentIndex, parentIndex + 1)
	pv.removeChildren(parentIndex + 1, parentIndex + 2)
	nv.entries = append(nv.entries, rv.entries...)
	nv.children = append(nv.children, rv.children...)
	for _, childPtr := range rv.children {
		ptr := tree.fetchW(childPtr)
		v := ptr.Get()
		v.Dirty(true)
		v.parent = nPtr.Ptr()
		ptr.Unlock()
	}

	tree.freeNode(rightPtr)
}

func (tree *BPlusTree) mergeNodeWithLeftInternal(parentIndex int, pPtr, nPtr, leftPtr cache.Pointable[*node]) {
	pv := pPtr.Get()
	nv := nPtr.Get()
	lv := leftPtr.Get()

	lv.appendEntry(pv.entries[parentIndex - 1])
	pv.removeEntries(parentIndex - 1, parentIndex)
	pv.removeChildren(parentIndex, parentIndex + 1)
	lv.entries = append(lv.entries, nv.entries...)
	lv.children = append(lv.children, nv.children...)
	for _, childPtr := range nv.children {
		ptr := tree.fetchW(childPtr)
		v := ptr.Get()
		v.Dirty(true)
		v.parent = leftPtr.Ptr()
		ptr.Unlock()
	}

	tree.freeNode(nPtr)
}

func (tree *BPlusTree) del(key [][]byte, nPtr cache.Pointable[*node]) bool {
	var pPtr cache.Pointable[*node]
	nv := nPtr.Get()
	if !nv.parent.IsNil() {
		pPtr = tree.fetchW(nv.parent)
	}

	if nv.isLeaf() {
		tree.removeFromLeaf(key, pPtr, nPtr);
	} else {
		tree.removeFromInternal(key, nPtr);
	}

	minCapacity := int(math.Ceil(float64(tree.meta.degree) / 2) - 1)

	if len(nv.entries) < minCapacity {
		if nPtr.Ptr().Addr() == tree.meta.root.Addr() {
			if len(nv.entries) == 0 && len(nv.children) != 0 {
				tree.meta.dirty = true
				tree.meta.root = nv.children[0]
				rPtr := tree.rootW()
				rv := rPtr.Get()
				rv.Dirty(true)
				rv.parent = tree.heap.Nil()
				tree.removeFromInternal(key, rPtr)
				rPtr.Unlock()
				tree.freeNode(nPtr)
			}

			nPtr.Unlock()
			return true
		}

		var rightPtr cache.Pointable[*node]
		var leftPtr cache.Pointable[*node]
		var rv *node
		var lv *node

		if !nv.right.IsNil() {
			rightPtr = tree.fetchW(nv.right)
			rv = rightPtr.Get()
		}
		if !nv.left.IsNil() {
			leftPtr = tree.fetchW(nv.left)
			lv = leftPtr.Get()
		}

		if nv.isLeaf() {
			if        rightPtr != nil && rv.parent.Addr() == nv.parent.Addr() && len(rv.entries) > minCapacity {
				tree.borrowKeyFromRightLeaf(pPtr, nPtr, rightPtr)
			} else if leftPtr != nil && lv.parent.Addr() == nv.parent.Addr() && len(lv.entries) > minCapacity {
				tree.borrowKeyFromLeftLeaf(pPtr, nPtr, leftPtr)
			} else if rightPtr != nil && rv.parent.Addr() == nv.parent.Addr() && len(rv.entries) <= minCapacity {
				tree.mergeNodeWithRightLeaf(pPtr, nPtr, rightPtr)
			} else if leftPtr != nil && lv.parent.Addr() == nv.parent.Addr() && len(lv.entries) <= minCapacity {
				tree.mergeNodeWithLeftLeaf(pPtr, nPtr, leftPtr, rightPtr)
			}
		} else {
			pv := pPtr.Get()
			parentIndex, _ := pv.search(nv.entries[len(nv.entries)-1].key)
			if pv.children[parentIndex].Addr() != nPtr.Ptr().Addr() {
				parentIndex = -1
			}

			if len(pv.children) > parentIndex + 1 {
				rightPtr = tree.fetchW(pv.children[parentIndex + 1])
				rv = rightPtr.Get()
			}
			if parentIndex > 0 {
				leftPtr = tree.fetchW(pv.children[parentIndex - 1])
				lv = leftPtr.Get()
			}

			if        rv != nil && rv.parent.Addr() == nv.parent.Addr() && len(rv.entries) > minCapacity {
				tree.borrowKeyFromRightInternal(parentIndex, pPtr, nPtr, rightPtr)
			} else if lv != nil && lv.parent.Addr() == nv.parent.Addr() && len(lv.entries) > minCapacity {
				tree.borrowKeyFromLeftInternal(parentIndex, pPtr, nPtr, leftPtr)
			} else if rv != nil && rv.parent.Addr() == nv.parent.Addr() && len(rv.entries) <= minCapacity {
				tree.mergeNodeWithRightInternal(parentIndex, pPtr, nPtr, rightPtr)
			} else if lv != nil && lv.parent.Addr() == nv.parent.Addr() && len(lv.entries) <= minCapacity {
				tree.mergeNodeWithLeftInternal(parentIndex, pPtr, nPtr, leftPtr)
			}
		}

		tree.removeFromInternal(key, nPtr)
		if rightPtr != nil {
			tree.removeFromInternal(key, rightPtr)
			rightPtr.Unlock()
		}
		if leftPtr != nil {
			tree.removeFromInternal(key, leftPtr)
			leftPtr.Unlock()
		}
	}

	nPtr.Unlock()
	if (pPtr != nil) {
		tree.del(key, pPtr);
	}

	return true
}

// searchRec searches the sub-tree with root 'n' recursively until the key
// is found or the leaf node is reached. Returns the node last searched,
// index where the key should be and a flag to indicate if the key exists.
func (tree *BPlusTree) searchRec(
	n cache.Pointable[*node],
	key [][]byte,
	flag cache.LOCKMODE,
) (
	ptr cache.Pointable[*node],
	index int,
	found bool,
) {
	for !n.Get().isLeaf() {
		index, found = n.Get().search(key)
		ptr = tree.fetchF(n.Get().children[index], flag)

		n.UnlockFlag(flag)
		n = ptr
	}

	index, found = n.Get().search(key)
	return n, index, found
}

// rightLeaf returns the right most leaf node of the sub-tree with given node
// as the root.
func (tree *BPlusTree) rightLeaf(n cache.Pointable[*node], flag cache.LOCKMODE) cache.Pointable[*node] {
	if n.Get().isLeaf() {
		return n
	}

	child := tree.fetchF(n.Get().children[len(n.Get().children) - 1], flag)
	n.UnlockFlag(flag)
	return tree.rightLeaf(child, flag)
}

// leftLeaf returns the left most leaf node of the sub-tree with given node
// as the root.
func (tree *BPlusTree) leftLeaf(n cache.Pointable[*node], flag cache.LOCKMODE) cache.Pointable[*node] {
	if n.Get().isLeaf() {
		return n
	}

	child := tree.fetchF(n.Get().children[0], flag)
	n.UnlockFlag(flag)
	return tree.leftLeaf(child, flag)
}

// fetch returns the node from given pointer. underlying file is accessed
// only if the node doesn't exist in cache.
func (tree *BPlusTree) fetchF(ptr allocator.Pointable, flag cache.LOCKMODE) cache.Pointable[*node] {
	nPtr := tree.cache.GetF(ptr, flag)
	if nPtr != nil {
		return nPtr
	}

	n := tree.newNode()
	if err := ptr.Get(n); err != nil {
		panic(errors.Wrap(err, "failed to get node data from pointer"))
	}

	n.Dirty(false)
	return tree.cache.AddF(ptr, flag)
}

func (tree *BPlusTree) fetchR(ptr allocator.Pointable) cache.Pointable[*node] {
	return tree.fetchF(ptr, cache.READ)
}

func (tree *BPlusTree) fetchW(ptr allocator.Pointable) cache.Pointable[*node] {
	return tree.fetchF(ptr, cache.WRITE)
}

func (tree *BPlusTree) rootF(flag cache.LOCKMODE) cache.Pointable[*node] {
	return tree.fetchF(tree.meta.root, flag)
}

func (tree *BPlusTree) rootR() cache.Pointable[*node] {
	return tree.rootF(cache.READ)
}

func (tree *BPlusTree) rootW() cache.Pointable[*node] {
	return tree.rootF(cache.WRITE)
}

func (tree *BPlusTree) newNode() *node {
	return &node{
		dirty:    true,
		meta:     tree.meta,
		dummyPtr: tree.heap.Nil(),
		right:    tree.heap.Nil(),
		left:     tree.heap.Nil(),
		parent:   tree.heap.Nil(),
		entries:  make([]entry, 0),
		children: make([]allocator.Pointable, 0),
	}
}

func (tree *BPlusTree) alloc(nt nodeType) cache.Pointable[*node] {
	var size uint32
	switch nt {
		case nodeLeaf: size = tree.leafNodeSize()
		case nodeInternal: size = tree.internalNodeSize()
		default: panic(fmt.Errorf("Invalid node type => %v", nt))
	}

	cPtr := tree.cache.AddW(tree.heap.Alloc(size))
	_ = cPtr.New() // in underhoods calls newNode method of bptree and assigns to pointer wrapper
	return cPtr
}

func (tree *BPlusTree) freeNode(ptr cache.Pointable[*node]) {
	rawPtr := ptr.Ptr()
	tree.cache.Del(rawPtr)
	tree.heap.Free(rawPtr)
}

// addCounterIfRequired adds extra counter bytes at end of key
// if Uniq option was set to False while creating BPTree
func (tree *BPlusTree) addCounterIfRequired(key [][]byte, flag counterOption) [][]byte {
	if tree.IsUniq() {
		return key
	}

	counter := make([]byte, 8)
	if flag == counterFill {
		bin.PutUint64(counter, math.MaxUint64)
	} else if flag == counterZero {
		bin.PutUint64(counter, 0)
	} else if flag == counterCurrent {
		bin.PutUint64(counter, tree.meta.counter)
	}

	return append(key, counter)
}

// reverse version of addCounterIfRequired
func (tree *BPlusTree) removeCounterIfRequired(key [][]byte) [][]byte {
	if tree.IsUniq() {
		return key
	}
	return key[:len(key)-1]
}

// open opens the B+ tree stored on disk using the heap.
// If heap is empty, a new B+ tree will be initialized.
func (tree *BPlusTree) open(opts *Options) error {
	firstPtr := tree.heap.FirstPointer(metadataSize)
	if tree.heap.Size() == firstPtr.Addr() - allocator.PointerMetaSize {
		// initialize a new B+ tree
		return tree.init(opts)
	}

	tree.meta = &metadata{
		root: tree.heap.Nil(),
	}
	tree.metaPtr = firstPtr
	if err := tree.metaPtr.Get(tree.meta); err != nil {
		return errors.Wrap(err, "failed to read meta while opening bptree")
	}

	// verify metadata
	if tree.meta.version != version {
		return fmt.Errorf("incompatible version %#x (expected: %#x)", tree.meta.version, version)
	}

	tree.cache.Add(tree.meta.root)
	return nil
}

func (tree *BPlusTree) leafNodeSize() uint32 {
	return uint32(leafNodeSize(
		int(tree.meta.degree),
		int(tree.meta.keySize),
		int(tree.meta.keyCols),
		int(tree.meta.valSize),
	))
}

func (tree *BPlusTree) internalNodeSize() uint32 {
	return uint32(internalNodeSize(
		int(tree.meta.degree),
		int(tree.meta.keySize),
		int(tree.meta.keyCols),
	))
}

// init initializes a new B+ tree in the underlying file. allocates 2 pages
// (1 for meta + 1 for root) and initializes the instance. metadata and the
// root node are expected to be written to file during insertion.
func (tree *BPlusTree) init(opts *Options) error {
	tree.meta = &metadata{
		dirty:    true,
		version:  version,
		flags:    0,
		size:     0,
		pageSize: uint32(opts.PageSize),
		keySize:  uint16(opts.MaxKeySize),
		keyCols:  uint16(opts.KeyCols),
		valSize:  uint16(opts.MaxValueSize),
		counter:  0,
		degree:   uint16(opts.Degree),
	}

	helpers.SetBit(&tree.meta.flags, uniquenessBit, opts.Uniq)
	if !opts.Uniq {
		// add extra column for counter to maintain uniqness
		tree.meta.keyCols++
		tree.meta.keySize += 8
	}

	tree.metaPtr = tree.heap.Alloc(metadataSize)

	rootPtr := tree.alloc(nodeLeaf)
	tree.meta.root = rootPtr.Ptr()
	rootPtr.Unlock()

	return errors.Wrap(tree.metaPtr.Set(tree.meta), "failed to write meta after init")
}

// writeAll writes all the nodes marked dirty to the underlying pager.
func (tree *BPlusTree) WriteAll() error {
	tree.cache.Flush()
	return tree.writeMeta()
}

func (tree *BPlusTree) writeMeta() error {
	if tree.meta.dirty {
		return tree.metaPtr.Set(tree.meta)
	}
	return nil
}
