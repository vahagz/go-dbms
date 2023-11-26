// Package bptree implements an on-disk B+ tree indexing scheme that can store
// key-value pairs and provide fast lookups and range scans. keys and values
// can be blobs binary data.
package bptree

import (
	"encoding/binary"
	"fmt"
	"math"
	"sync"

	allocator "go-dbms/pkg/allocator/heap"
	"go-dbms/pkg/cache"
	"go-dbms/pkg/customerrors"
	"go-dbms/pkg/pager"

	"github.com/pkg/errors"
)

// bin is the byte order used for all marshals/unmarshals.
var bin = binary.LittleEndian

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

	tree.cache = cache.NewCache[*node](1000, tree.newNode)

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
	root  cache.Pointable[*node]
}

// Get fetches the value associated with the given key. Returns error if key
// not found.
func (tree *BPlusTree) Get(key [][]byte) ([][]byte, error) {
	if len(key) == 0 {
		return nil, customerrors.ErrEmptyKey
	}

	tree.mu.RLock()
	defer tree.mu.RUnlock()

	if len(tree.root.Get().entries) == 0 {
		return nil, customerrors.ErrKeyNotFound
	}

	tree.root.RLock()
	n, startIdx, endIdx, found, err := tree.searchRec(tree.root, key, cache.READ)
	defer n.RUnlock()
	if err != nil {
		return nil, err
	} else if !found {
		return nil, customerrors.ErrKeyNotFound
	}

	res := make([][]byte, 0, endIdx - startIdx)
	for idx := startIdx; idx <= endIdx; idx++ {
		res = append(res, n.Get().entries[idx].val)
	}
	return res, nil
}

// Put puts the key-value pair into the B+ tree. If the key already exists,
// its value will be updated.
func (tree *BPlusTree) Put(key [][]byte, val []byte, opt *PutOptions) error {
	err := tree.PutMem(key, val, opt)
	if err != nil {
		return err
	}

	return tree.WriteAll()
}

func (tree *BPlusTree) PutMem(key [][]byte, val []byte, opt *PutOptions) error {
	keylen := 0
	for _, v := range key {
		keylen += len(v)
	}

	if keylen > int(tree.meta.keySize) {
		return customerrors.ErrKeyTooLarge
	} else if keylen == 0 {
		return customerrors.ErrEmptyKey
	}

	tree.mu.Lock()
	defer tree.mu.Unlock()

	e := entry{
		key: key,
		val: val,
	}

	isInsert, err := tree.insert(e, opt)
	if err != nil {
		return err
	}

	if isInsert {
		tree.meta.size++
		tree.meta.dirty = true
	}

	return nil
}

// Del removes the key-value entry from the B+ tree. If the key does not
// exist, returns error.
func (tree *BPlusTree) Del(key [][]byte) ([][]byte, error) {
	tree.mu.Lock()
	defer tree.mu.Unlock()

	target, startIdx, endIdx, found, err := tree.searchRec(tree.root, key, cache.WRITE)
	if err != nil {
		return nil, err
	} else if !found {
		return nil, customerrors.ErrKeyNotFound
	}

	valArr := make([][]byte, 0, endIdx - startIdx)
	removed := target.Get().remove(startIdx, endIdx)
	for _, e := range removed {
		valArr = append(valArr, e.val)
	}
	return valArr, tree.WriteAll()
}

// Scan performs an index scan starting at the given key. Each entry will be
// passed to the scanFn. If the key is zero valued (nil or len=0), then the
// left/right leaf key will be used as the starting key. Scan continues until
// the right most leaf node is reached or the scanFn returns 'true' indicating
// to stop the scan. If reverse=true, scan starts at the right most node and
// executes in descending order of keys.
func (tree *BPlusTree) Scan(
	key [][]byte,
	reverse, strict bool,
	scanFn func(key [][]byte, val []byte) (bool, error),
) error {
	tree.mu.RLock()
	defer tree.mu.RUnlock()

	if tree.meta.size == 0 {
		return nil
	}

	var err error
	var beginAt cache.Pointable[*node]
	startIdx := 0
	endIdx := 0
	idx := 0

	tree.root.RLock()
	if len(key) == 0 {
		// No explicit key provided by user, find the a leaf-node based on
		// scan direction and start there.
		if !reverse {
			beginAt, err = tree.leftLeaf(tree.root, cache.READ)
			idx = 0
		} else {
			beginAt, err = tree.rightLeaf(tree.root, cache.READ)
			idx = len(beginAt.Get().entries) - 1
		}
	} else {
		// we have a specific key to start at. find the node containing the
		// key and start the scan there.
		beginAt, startIdx, endIdx, _, err = tree.searchRec(tree.root, key, cache.READ)
		if !reverse {
			if strict {
				idx = startIdx
			} else {
				idx = endIdx + 1
			}
		} else {
			if strict {
				idx = endIdx
			} else {
				idx = startIdx - 1
			}
		}
	}

	if err != nil {
		return err
	}

	// starting at found leaf node, follow the 'next' pointer until.
	var nextNode allocator.Pointable

	L: for beginAt != nil {
		if !reverse {
			for i := idx; i < len(beginAt.Get().entries); i++ {
				e := beginAt.Get().entries[i]
				if stop, err := scanFn(e.key, e.val); err != nil {
					beginAt.RUnlock()
					return err
				} else if stop {
					beginAt.RUnlock()
					break L
				}
			}
			nextNode = beginAt.Get().next
		} else {
			for i := idx; i >= 0; i-- {
				e := beginAt.Get().entries[i]
				if stop, err := scanFn(e.key, e.val); err != nil {
					beginAt.RUnlock()
					return err
				} else if stop {
					beginAt.RUnlock()
					break L
				}
			}
			nextNode = beginAt.Get().prev
		}
		idx = 0

		if nextNode.IsNil() {
			beginAt.RUnlock()
			break
		}

		beginAt.RUnlock()
		beginAt, err = tree.fetch(nextNode, cache.READ)
		if err != nil {
			return errors.Wrap(err, "failed to fetch next node")
		}
	}

	return nil
}

func (tree *BPlusTree) PrepareSpace(size uint32) error {
	return tree.heap.PreAlloc(size)
}

func (tree *BPlusTree) Count() (int, error) {
	counter := 0
	err := tree.Scan(nil, false, true, func(_ [][]byte, _ []byte) (bool, error) {
		counter++
		return false, nil
	})
	return counter, err
}

// Size returns the number of entries in the entire tree
func (tree *BPlusTree) Size() int64 { return int64(tree.meta.size) }

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
	tree.print(tree.root, 0)
}

func (tree *BPlusTree) ClearCache() {
	tree.cache.Clear()
}

func (tree *BPlusTree) print(nPtr cache.Pointable[*node], indent int) {
	n := nPtr.Get()
	for i := len(n.entries) - 1; i >= 0; i-- {
		if !n.isLeaf() {
			n, _ := tree.fetch(n.children[i+1], cache.NONE)
			tree.print(n, indent + 4)
		}
		var parentPtr uint64
		if !nPtr.Get().parent.IsNil() {
			parentPtr = tree.fetchE(nPtr.Get().parent, cache.NONE).Ptr().Addr()
		}
		// binary.BigEndian.Uint32(n.entries[i].key[0])
		fmt.Printf("%*s%v(%v)\n", indent, "", n.entries[i].key, parentPtr)
	}

	if !n.isLeaf() {
		n, _ := tree.fetch(n.children[0], cache.NONE)
		tree.print(n, indent + 4)
	}
}

func (tree *BPlusTree) insert(e entry, opt *PutOptions) (bool, error) {
	tree.root.Lock()
	leaf, start, end, found, err := tree.searchRec(tree.root, e.key, cache.WRITE)
	if err != nil {
		return false, errors.Wrap(err, "failed to find leaf node to insert entry")
	}
	
	if opt.Uniq && found && !opt.Update {
		return false, errors.New("key already exists")
	} else if found && opt.Update {
		for i := start; i <= end; i++ {
			leaf.Get().update(i, e.val)
		}
		return false, nil
	}

	leaf.Get().insertAt(start, e)
	if leaf.Get().IsFull() {
		return true, tree.split(leaf)
	}
	leaf.Unlock()

	return true, nil
}

func (tree *BPlusTree) split(nPtr cache.Pointable[*node]) (err error) {
	nv := nPtr.Get()
	var siblingPtr cache.Pointable[*node]
	if nv.isLeaf() {
		siblingPtr, err = tree.allocLeaf()
	} else {
		siblingPtr, err = tree.allocInternal()
	}
	if err != nil {
		return errors.Wrap(err, "failed to alloc node")
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

		sv.next = nv.next
		sv.prev = nPtr.Ptr()
		nv.next = siblingPtr.Ptr()
		if !sv.next.IsNil() {
			nNext, err := tree.fetch(sv.next, cache.WRITE)
			if err != nil {
				return errors.Wrap(err, "failed to fetch next")
			}
			nNext.Get().Dirty(true)
			nNext.Get().prev = siblingPtr.Ptr()
			nNext.Unlock()
		}
	} else {
		sv.entries = make([]entry, 0, tree.meta.degree)
		sv.entries = append(sv.entries, nv.entries[breakPoint+1:]...)
		sv.children = make([]allocator.Pointable, 0, tree.meta.degree+1)
		sv.children = append(sv.children, nv.children[breakPoint+1:]...)
		for _, sChildPtr := range sv.children {
			sChild, err := tree.fetch(sChildPtr, cache.WRITE)
			if err != nil {
				return errors.Wrap(err, "failed to fetch sibling child")
			}

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
		tree.root, err = tree.allocInternal()
		if err != nil {
			return errors.Wrap(err, "failed to alloc new root")
		}

		tree.meta.dirty = true
		tree.meta.root = tree.root.Ptr()
		pPtr = tree.root
		sv.parent = pPtr.Ptr()
		nv.parent = pPtr.Ptr()
		pPtr.Get().insertChild(0, nPtr.Ptr())
	} else {
		pPtr, err = tree.fetch(nv.parent, cache.WRITE)
		if err != nil {
			return errors.Wrap(err, "failed to fetch parent")
		}
	}

	pv := pPtr.Get()
	pv.Dirty(true)
	start, _, _ := pv.search(pe.key)
	pv.insertAt(start, pe)
	pv.insertChild(start + 1, siblingPtr.Ptr())

	if pv.parent.IsNil() {
		tree.root = pPtr
		tree.meta.dirty = true
		tree.meta.root = pPtr.Ptr()
	}

	nPtr.Unlock()
	siblingPtr.Unlock()
	if pv.IsFull() {
		return tree.split(pPtr)
	}
	pPtr.Unlock()
	return nil
}

// searchRec searches the sub-tree with root 'n' recursively until the key
// is  found or the leaf node is  reached. Returns the node last searched,
// index where the key should be and a flag to indicate if the key exists.
func (tree *BPlusTree) searchRec(
	n cache.Pointable[*node],
	key [][]byte,
	flag cache.LOCKMODE,
) (
	ptr cache.Pointable[*node],
	startIndex int,
	endIndex int,
	found bool,
	err error,
) {
	startIndex, endIndex, found = n.Get().search(key)

	if n.Get().isLeaf() {
		return n, startIndex, endIndex, found, nil
	}

	if found {
		endIndex++
	}

	child, err := tree.fetch(n.Get().children[endIndex], flag)
	n.UnlockFlag(flag)
	if err != nil {
		return nil, 0, 0, false, errors.Wrap(err, "failed to get child")
	}
	return tree.searchRec(child, key, flag)
}

// rightLeaf returns the right most leaf node of the sub-tree with given node
// as the root.
func (tree *BPlusTree) rightLeaf(n cache.Pointable[*node], flag cache.LOCKMODE) (cache.Pointable[*node], error) {
	if n.Get().isLeaf() {
		return n, nil
	}

	lastChildIdx := len(n.Get().children) - 1
	child, err := tree.fetch(n.Get().children[lastChildIdx], flag)
	if err != nil {
		return nil, errors.Wrap(err, "failed to fetch child")
	}
	return tree.rightLeaf(child, flag)
}

// leftLeaf returns the left most leaf node of the sub-tree with given node
// as the root.
func (tree *BPlusTree) leftLeaf(n cache.Pointable[*node], flag cache.LOCKMODE) (cache.Pointable[*node], error) {
	if n.Get().isLeaf() {
		return n, nil
	}

	child, err := tree.fetch(n.Get().children[0], flag)
	if err != nil {
		return nil, errors.Wrap(err, "failed to fetch child")
	}
	n.UnlockFlag(flag)
	return tree.leftLeaf(child, flag)
}

// fetch returns the node from given pointer. underlying file is accessed
// only if the node doesn't exist in cache.
func (tree *BPlusTree) fetch(ptr allocator.Pointable, flag cache.LOCKMODE) (cache.Pointable[*node], error) {
	var nPtr cache.Pointable[*node]
	switch flag {
		case cache.READ:
			nPtr = tree.cache.GetR(ptr)
			break
		case cache.WRITE:
			nPtr = tree.cache.GetW(ptr)
			break
		case cache.NONE:
			nPtr = tree.cache.Get(ptr)
			break
	}
	if nPtr != nil {
		return nPtr, nil
	}

	n := tree.newNode()
	if err := ptr.Get(n); err != nil {
		return nil, errors.Wrap(err, "failed to get node data from pointer")
	}

	n.Dirty(false)
	switch flag {
		case cache.READ: return tree.cache.AddR(ptr), nil
		case cache.WRITE: return tree.cache.AddW(ptr), nil
		case cache.NONE: return tree.cache.Add(ptr), nil
	}
	return nil, nil
}

func (tree *BPlusTree) fetchE(ptr allocator.Pointable, flag cache.LOCKMODE) cache.Pointable[*node] {
	v, err := tree.fetch(ptr, flag)
	if err != nil {
		panic(err)
	}
	return v
}

func (tree *BPlusTree) newNode() *node {
	return &node{
		dirty:    true,
		meta:     tree.meta,
		dummyPtr: tree.heap.Nil(),
		next:     tree.heap.Nil(),
		prev:     tree.heap.Nil(),
		parent:   tree.heap.Nil(),
		entries:  make([]entry, 0),
		children: make([]allocator.Pointable, 0),
	}
}

func (tree *BPlusTree) allocLeaf() (cache.Pointable[*node], error) {
	ptr, err := tree.heap.Alloc(tree.leafNodeSize())
	if err != nil {
		return nil, errors.Wrap(err, "failed to alloc leaf node")
	}
	cPtr := tree.cache.AddW(ptr)
	val := cPtr.Get()
	val.Dirty(true)
	val.meta = tree.meta
	val.next = tree.heap.Nil()
	val.prev = tree.heap.Nil()
	val.parent = tree.heap.Nil()
	val.dummyPtr = tree.heap.Nil()
	val.children = make([]allocator.Pointable, 0)
	val.entries = make([]entry, 0)
	return cPtr, nil
}

func (tree *BPlusTree) allocInternal() (cache.Pointable[*node], error) {
	ptr, err := tree.heap.Alloc(tree.internalNodeSize())
	if err != nil {
		return nil, errors.Wrap(err, "failed to alloc leaft node")
	}
	cPtr := tree.cache.AddW(ptr)
	val := cPtr.Get()
	val.Dirty(true)
	val.meta = tree.meta
	val.next = tree.heap.Nil()
	val.prev = tree.heap.Nil()
	val.parent = tree.heap.Nil()
	val.dummyPtr = tree.heap.Nil()
	val.children = make([]allocator.Pointable, 0)
	val.entries = make([]entry, 0)
	return cPtr, nil
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

	tree.root = tree.cache.Add(tree.meta.root)
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
		degree:   uint16(opts.Degree),
	}

	metaPtr, err := tree.heap.Alloc(metadataSize)
	if err != nil {
		return err
	}
	tree.metaPtr = metaPtr

	rootPtr, err := tree.allocLeaf()
	if err != nil {
		return errors.Wrap(err, "failed to alloc root node")
	}
	tree.meta.root = rootPtr.Ptr()
	tree.root = tree.cache.Add(tree.meta.root)
	tree.root.Unlock()

	return errors.Wrap(metaPtr.Set(tree.meta), "failed to write meta after init")
}

// writeAll writes all the nodes marked dirty to the underlying pager.
func (tree *BPlusTree) WriteAll() error {
	tree.root.Flush()
	tree.cache.Flush()
	return tree.writeMeta()
}

func (tree *BPlusTree) writeMeta() error {
	if tree.meta.dirty {
		return tree.metaPtr.Set(tree.meta)
	}
	return nil
}
