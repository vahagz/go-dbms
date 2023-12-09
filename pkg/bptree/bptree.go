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

	tree.cache = cache.NewCache[*node](100, tree.newNode)

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
func (tree *BPlusTree) Put(key [][]byte, val []byte, opt *PutOptions) error {
	err := tree.PutMem(key, val, opt)
	if err != nil {
		return err
	}

	return tree.WriteAll()
}

func (tree *BPlusTree) PutMem(key [][]byte, val []byte, opt *PutOptions) error {
	key = tree.addCounterIfRequired(helpers.Copy(key), counterCurrent)
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

	isInsert, err := tree.put(e, opt)
	if err != nil {
		return err
	}

	if isInsert {
		tree.meta.counter++
		tree.meta.size++
		tree.meta.dirty = true
	}

	return nil
}

// Del removes the key-value entry from the B+ tree. If the key does not
// exist, returns error.
func (tree *BPlusTree) Del(key [][]byte) ([]byte, error) {
	tree.mu.Lock()
	defer tree.mu.Unlock()

	key = helpers.Copy(key)
	root := tree.rootW()
	target, index, found, err := tree.searchRec(root, key, cache.WRITE)
	if err != nil {
		return nil, err
	} else if !found {
		return nil, customerrors.ErrKeyNotFound
	}

	// if isDelete {
	// 	tree.meta.size--
	// 	tree.meta.dirty = true
	// }

	removed := target.Get().remove(index, index + 1)
	return removed[0].val, tree.WriteAll()
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

	var err error
	var beginAt cache.Pointable[*node]
	idx := 0

	root := tree.rootR()
	if len(key) == 0 {
		// No explicit key provided by user, find the a leaf-node based on
		// scan direction and start there.
		if !opts.Reverse {
			beginAt, err = tree.leftLeaf(root, cache.READ)
			idx = 0
		} else {
			beginAt, err = tree.rightLeaf(root, cache.READ)
			idx = len(beginAt.Get().entries) - 1
		}
	} else {
		// we have a specific key to start at. find the node containing the
		// key and start the scan there.

		key = helpers.Copy(key)
		if (opts.Strict && opts.Reverse) || (!opts.Strict && !opts.Reverse) {
			key = tree.addCounterIfRequired(key, counterFill)
		} else {
			key = tree.addCounterIfRequired(key, counterZero)
		}

		beginAt, idx, _, err = tree.searchRec(root, key, cache.READ)
		if opts.Reverse {
			idx--
		}
	}

	if err != nil {
		return err
	}

	// starting at found leaf node, follow the 'next' pointer until.
	var nextNode allocator.Pointable

	L: for beginAt != nil {
		if !opts.Reverse {
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
			nextNode = beginAt.Get().right
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
			nextNode = beginAt.Get().left
		}
		idx = 0

		beginAt.RUnlock()
		if nextNode.IsNil() {
			break
		}

		beginAt = tree.fetchR(nextNode)
		if !opts.Reverse {
			idx = 0
		} else {
			idx = len(beginAt.Get().entries) - 1
		}
	}

	return nil
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
	tree.print(tree.rootR(), 0)
}

func (tree *BPlusTree) ClearCache() {
	tree.cache.Clear()
}

func (tree *BPlusTree) print(nPtr cache.Pointable[*node], indent int) {
	n := nPtr.Get()
	for i := len(n.entries) - 1; i >= 0; i-- {
		if !n.isLeaf() {
			tree.print(tree.fetchR(n.children[i+1]), indent + 4)
		}
		var parentPtr uint64
		if !nPtr.Get().parent.IsNil() {
			parentPtr = tree.fetchR(nPtr.Get().parent).Ptr().Addr()
		}
		// binary.BigEndian.Uint32(n.entries[i].key[0])
		fmt.Printf("%*s%v(%v)\n", indent, "", n.entries[i].key, parentPtr)
	}

	if !n.isLeaf() {
		tree.print(tree.fetchR(n.children[0]), indent + 4)
	}
}

func (tree *BPlusTree) put(e entry, opt *PutOptions) (bool, error) {
	root := tree.rootW()
	leaf, index, found, err := tree.searchRec(root, e.key, cache.WRITE)
	if err != nil {
		return false, errors.Wrap(err, "failed to find leaf node to insert entry")
	}

	if found && !opt.Update {
		return false, errors.New("key already exists")
	} else if found && opt.Update {
		leaf.Get().update(index, e.val)
		return false, nil
	}

	leaf.Get().insertAt(index, e)
	if leaf.Get().IsFull() {
		tree.split(leaf)
		return true, nil
	}
	leaf.Unlock()

	return true, nil
}

func (tree *BPlusTree) split(nPtr cache.Pointable[*node]) {
	nv := nPtr.Get()
	var siblingPtr cache.Pointable[*node]
	if nv.isLeaf() {
		siblingPtr = tree.allocLeaf()
	} else {
		siblingPtr = tree.allocInternal()
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
		pPtr = tree.allocInternal()
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
	pv.insertAt(index, pe)
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
	err error,
) {
	for !n.Get().isLeaf() {
		index, found = n.Get().search(key)
		ptr = tree.fetchF(n.Get().children[index], flag)

		n.UnlockFlag(flag)
		n = ptr
	}

	index, found = n.Get().search(key)
	return n, index, found, nil
}

// rightLeaf returns the right most leaf node of the sub-tree with given node
// as the root.
func (tree *BPlusTree) rightLeaf(n cache.Pointable[*node], flag cache.LOCKMODE) (cache.Pointable[*node], error) {
	if n.Get().isLeaf() {
		return n, nil
	}

	child := tree.fetchF(n.Get().children[len(n.Get().children) - 1], flag)
	n.UnlockFlag(flag)
	return tree.rightLeaf(child, flag)
}

// leftLeaf returns the left most leaf node of the sub-tree with given node
// as the root.
func (tree *BPlusTree) leftLeaf(n cache.Pointable[*node], flag cache.LOCKMODE) (cache.Pointable[*node], error) {
	if n.Get().isLeaf() {
		return n, nil
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

func (tree *BPlusTree) allocLeaf() cache.Pointable[*node] {
	cPtr := tree.cache.AddW(tree.heap.Alloc(tree.leafNodeSize()))
	val := cPtr.Get()
	val.Dirty(true)
	val.meta = tree.meta
	val.right = tree.heap.Nil()
	val.left = tree.heap.Nil()
	val.parent = tree.heap.Nil()
	val.dummyPtr = tree.heap.Nil()
	val.children = make([]allocator.Pointable, 0)
	val.entries = make([]entry, 0)
	return cPtr
}

func (tree *BPlusTree) allocInternal() cache.Pointable[*node] {
	cPtr := tree.cache.AddW(tree.heap.Alloc(tree.internalNodeSize()))
	val := cPtr.Get()
	val.Dirty(true)
	val.meta = tree.meta
	val.right = tree.heap.Nil()
	val.left = tree.heap.Nil()
	val.parent = tree.heap.Nil()
	val.dummyPtr = tree.heap.Nil()
	val.children = make([]allocator.Pointable, 0)
	val.entries = make([]entry, 0)
	return cPtr
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

	rootPtr := tree.allocLeaf()
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
