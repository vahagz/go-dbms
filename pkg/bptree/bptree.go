// Package bptree implements an on-disk B+ tree indexing scheme that can store
// key-value pairs and provide fast lookups and range scans. keys and values
// can be blobs binary data.
package bptree

import (
	"encoding/binary"
	"fmt"
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

	n, startIdx, endIdx, found, err := tree.searchRec(tree.root, key)
	if err != nil {
		return nil, err
	} else if !found {
		return nil, customerrors.ErrKeyNotFound
	}

	res := make([][]byte, 0, endIdx - startIdx)
	for idx := startIdx; idx < endIdx; idx++ {
		res = append(res, n.Get().entries[idx].val)
	}
	return res, nil
}

// Put puts the key-value pair into the B+ tree. If the key already exists,
// its value will be updated.
func (tree *BPlusTree) Put(key [][]byte, val []byte, opt *PutOptions) error {
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
		tree.meta.size++
		tree.meta.dirty = true
	}

	return tree.writeAll()
}

// Del removes the key-value entry from the B+ tree. If the key does not
// exist, returns error.
func (tree *BPlusTree) Del(key [][]byte) ([][]byte, error) {
	tree.mu.Lock()
	defer tree.mu.Unlock()

	target, startIdx, endIdx, found, err := tree.searchRec(tree.root, key)
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
	return valArr, tree.writeAll()
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
	defer tree.root.RUnlock()
	if len(key) == 0 {
		// No explicit key provided by user, find the a leaf-node based on
		// scan direction and start there.
		if !reverse {
			beginAt, err = tree.leftLeaf(tree.root)
			idx = 0
		} else {
			beginAt, err = tree.rightLeaf(tree.root)
			idx = len(beginAt.Get().entries) - 1
		}
	} else {
		// we have a specific key to start at. find the node containing the
		// key and start the scan there.
		beginAt, startIdx, endIdx, _, err = tree.searchRec(tree.root, key)
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
					return err
				} else if stop {
					break L
				}
			}
			nextNode = beginAt.Get().next
		} else {
			for i := idx; i >= 0; i-- {
				e := beginAt.Get().entries[i]
				if stop, err := scanFn(e.key, e.val); err != nil {
					return err
				} else if stop {
					break L
				}
			}
			nextNode = beginAt.Get().prev
		}
		idx = 0

		if nextNode.IsNil() {
			break
		}

		beginAt, err = tree.fetch(nextNode)
		if err != nil {
			return errors.Wrap(err, "failed to fetch next node")
		}
	}

	return nil
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

	_ = tree.writeAll() // write if any nodes are pending
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

func (tree *BPlusTree) put(e entry, opt *PutOptions) (bool, error) {
	tree.root.Lock()
	defer tree.root.Unlock()

	if tree.isFull(tree.root.Get()) {
		// we will need 2 extra nodes for splitting the root
		// (1 to act as new root + 1 for the right sibling)
		newRoot, err := tree.allocInternal()
		if err != nil {
			return false, errors.Wrap(err, "failed to alloc new root")
		}

		var rightSibling cache.Pointable[*node]
		oldRoot := tree.root
		if oldRoot.Get().isLeaf() {
			rightSibling, err = tree.allocLeaf()
		} else {
			rightSibling, err = tree.allocInternal()
		}

		if err != nil {
			return false, errors.Wrap(err, "failed to alloc new root right sibling")
		}

		// update the tree root
		newRootV := newRoot.Get()
		newRootV.children = append(newRootV.children, oldRoot.Ptr())
		tree.root = newRoot
		tree.meta.root = newRoot.Ptr()

		if err := tree.split(newRoot, oldRoot, rightSibling, 0); err != nil {
			return false, err
		}
	}

	return tree.insertNonFull(tree.root, e, opt)
}

func (tree *BPlusTree) insertNonFull(
	n cache.Pointable[*node],
	e entry,
	opt *PutOptions,
) (bool, error) {
	nv := n.Get()
	if nv.isLeaf() {
		startIdx, endIdx, found := nv.search(e.key)

		if opt.Uniq && found && !opt.Update {
			return false, errors.New("key already exists")
		} else if found && opt.Update {
			for idx := startIdx; idx <= endIdx; idx++ {
				nv.update(idx, e.val)
			}
			return false, nil
		}

		nv.insertAt(startIdx, e)
		return true, nil
	}

	return tree.insertInternal(n, e, opt)
}

func (tree *BPlusTree) insertInternal(
	n cache.Pointable[*node],
	e entry,
	opt *PutOptions,
) (bool, error) {
	nv := n.Get()
	_, endIdx, found := nv.search(e.key)
	if found {
		endIdx++
	}

	child, err := tree.fetch(nv.children[endIdx])
	if err != nil {
		return false, errors.Wrap(err, "failed to fetch child")
	}
	childV := child.Get()

	if tree.isFull(childV) {
		var sibling cache.Pointable[*node]
		var err error
		if childV.isLeaf() {
			sibling, err = tree.allocLeaf()
		} else {
			sibling, err = tree.allocInternal()
		}

		if err != nil {
			return false, errors.Wrap(err, "failed to alloc node to split")
		}

		if err := tree.split(n, child, sibling, endIdx); err != nil {
			return false, errors.Wrap(err, "failed to split node")
		}

		// should go into left child or right child?
		if helpers.CompareMatrix(e.key, nv.entries[endIdx].key) >= 0 {
			child, err = tree.fetch(nv.children[endIdx+1])
			if err != nil {
				return false, errors.Wrap(err, "failed to fetch child")
			}
		}
	}

	return tree.insertNonFull(child, e, opt)
}

func (tree *BPlusTree) split(p, n, sibling cache.Pointable[*node], i int) error {
	pv := p.Get()
	nv := n.Get()
	siblingV := sibling.Get()

	pv.Dirty(true)
	nv.Dirty(true)
	siblingV.Dirty(true)

	if nv.isLeaf() {
		// split leaf node. use 'sibling' as the right node for 'n'.
		siblingV.next = nv.next
		siblingV.prev = n.Ptr()
		nv.next = sibling.Ptr()

		siblingV.entries = make([]entry, tree.meta.degree-1)
		copy(siblingV.entries, nv.entries[tree.meta.degree-1:])
		nv.entries = nv.entries[:tree.meta.degree-1]

		pv.insertChild(i+1, sibling.Ptr())
		pv.insertAt(i, siblingV.entries[0])
	} else {
		// split internal node. use 'sibling' as left node for 'n'.
		parentKey := nv.entries[tree.meta.degree-1]

		siblingV.entries = make([]entry, tree.meta.degree-1)
		copy(siblingV.entries, nv.entries[:tree.meta.degree])
		nv.entries = nv.entries[tree.meta.degree:]

		siblingV.children = make([]allocator.Pointable, tree.meta.degree)
		copy(siblingV.children, nv.children[:tree.meta.degree])
		nv.children = nv.children[tree.meta.degree:]

		pv.insertChild(i, sibling.Ptr())
		pv.insertAt(i, parentKey)
	}

	return nil
}

// searchRec searches the sub-tree with root 'n' recursively until the key
// is  found or the leaf node is  reached. Returns the node last searched,
// index where the key should be and a flag to indicate if the key exists.
func (tree *BPlusTree) searchRec(
	n cache.Pointable[*node],
	key [][]byte,
) (cache.Pointable[*node], int, int, bool, error) {
	startIdx, endIdx, found := n.Get().search(key)

	if found {
		endIdx++
	}

	if n.Get().isLeaf() {
		return n, startIdx, endIdx, found, nil
	}

	child, err := tree.fetch(n.Get().children[endIdx])
	if err != nil {
		return nil, 0, 0, false, errors.Wrap(err, "failed to get child")
	}
	return tree.searchRec(child, key)
}

// rightLeaf returns the right most leaf node of the sub-tree with given node
// as the root.
func (tree *BPlusTree) rightLeaf(n cache.Pointable[*node]) (cache.Pointable[*node], error) {
	if n.Get().isLeaf() {
		return n, nil
	}

	lastChildIdx := len(n.Get().children) - 1
	child, err := tree.fetch(n.Get().children[lastChildIdx])
	if err != nil {
		return nil, errors.Wrap(err, "failed to fetch child")
	}
	return tree.rightLeaf(child)
}

// leftLeaf returns the left most leaf node of the sub-tree with given node
// as the root.
func (tree *BPlusTree) leftLeaf(n cache.Pointable[*node]) (cache.Pointable[*node], error) {
	if n.Get().isLeaf() {
		return n, nil
	}

	child, err := tree.fetch(n.Get().children[0])
	if err != nil {
		return nil, errors.Wrap(err, "failed to fetch child")
	}
	return tree.leftLeaf(child)
}

func (tree *BPlusTree) isFull(n *node) bool {
	return len(n.entries) == int(tree.meta.degree - 1)
}

// fetch returns the node from given pointer. underlying file is accessed
// only if the node doesn't exist in cache.
func (tree *BPlusTree) fetch(ptr allocator.Pointable) (cache.Pointable[*node], error) {
	nPtr := tree.cache.Get(ptr)
	if nPtr != nil {
		return nPtr, nil
	}

	n := tree.newNode()
	if err := ptr.Get(n); err != nil {
		return nil, errors.Wrap(err, "failed to get node data from pointer")
	}

	n.Dirty(false)
	return tree.cache.Add(ptr), nil
}

func (tree *BPlusTree) newNode() *node {
	return &node{
		dirty:    true,
		next:     tree.heap.Nil(),
		prev:     tree.heap.Nil(),
		entries:  make([]entry, 0),
		children: make([]allocator.Pointable, 0),
	}
}

func (tree *BPlusTree) allocLeaf() (cache.Pointable[*node], error) {
	ptr, err := tree.heap.Alloc(tree.leafNodeSize())
	if err != nil {
		return nil, errors.Wrap(err, "failed to alloc leaft node")
	}
	cPtr := tree.cache.Add(ptr)
	cPtr.Get().next = tree.heap.Nil()
	cPtr.Get().prev = tree.heap.Nil()
	return cPtr, nil
}

func (tree *BPlusTree) allocInternal() (cache.Pointable[*node], error) {
	ptr, err := tree.heap.Alloc(tree.internalNodeSize())
	if err != nil {
		return nil, errors.Wrap(err, "failed to alloc leaft node")
	}
	cPtr := tree.cache.Add(ptr)
	cPtr.Get().next = tree.heap.Nil()
	cPtr.Get().prev = tree.heap.Nil()
	return cPtr, nil
}

// // allocOne allocates a page in the underlying pager and creates a node
// // on that page. node is not written to the page in this call.
// func (tree *BPlusTree) allocOne() (*node, error) {
// 	nodes, err := tree.alloc(1)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return nodes[0], nil
// }

// // alloc allocates pages required for 'n' new nodes. alloc will reuse
// // pages from free-list if available.
// func (tree *BPlusTree) alloc(n int) ([]*node, error) {
// 	// check if there are enough free pages from the freelist
// 	// and try to allocate sequential set of 
// 	var pid uint64
// 	pidPtr, rem := allocSeq(tree.meta.freeList, n)
// 	tree.meta.freeList = rem

// 	// free list could be having less pages than we actually need.
// 	// we need to allocate if that is the case.
// 	if pidPtr == nil {
// 		var err error
// 		pid, err = tree.pager.Alloc(n)
// 		if err != nil {
// 			return nil, err
// 		}
// 	} else {
// 		pid = *pidPtr
// 	}

// 	nodes := make([]*node, n)
// 	for i := 0; i < n; i++ {
// 		n := newNode(pid)
// 		tree.nodes[pid] = n
// 		nodes[i] = n
// 		pid++
// 	}

// 	return nodes, nil
// }

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

	return errors.Wrap(metaPtr.Set(tree.meta), "failed to write meta after init")
}

// writeAll writes all the nodes marked dirty to the underlying pager.
func (tree *BPlusTree) writeAll() error {
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

// allocSeq finds a subset of size 'n' in 'free' that is sequential.
// Returns the first int in the sequence the set after removing the
// subset.
func allocSeq(free []uint64, n int) (id *uint64, remaining []uint64) {
	if len(free) <= n {
		return nil, free
	} else if n == 1 {
		return &free[0], free[1:]
	}

	i, j := 0, 0
	for ; i < len(free); i++ {
		j = i + (n - 1)
		if j < len(free) && free[j] == free[i]+uint64(n-1) {
			break
		}
	}

	if i >= len(free) || j >= len(free) {
		return nil, free
	}

	id = &free[i]
	free = append(free[:i], free[j+1:]...)
	return id, free
}
