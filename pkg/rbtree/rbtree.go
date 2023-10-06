package rbtree

import (
	"encoding/binary"
	"go-dbms/pkg/pager"
)

var bin = binary.LittleEndian

func Open(fileName string, opts *Options) (*RBTree, error) {
	p, err := pager.Open(fileName, int(opts.PageSize), false, 0664)
	if err != nil {
		return nil, err
	}

	tree := &RBTree{
		file:     fileName,
		pager:    p,
		root:     nil,
		pages:    map[uint64]*page{},
		degree:   opts.PageSize / (nodeFixedSize + opts.KeySize),
		nodeSize: nodeFixedSize + opts.KeySize,
		meta:     &metadata{},
	}

	if err := tree.open(opts); err != nil {
		_ = tree.Close()
		return nil, err
	}

	return tree, nil
}

// RBTree represents an on-disk B+ tree. Each node in the tree is mapped
// to a single page in the file. Degree of the tree is decided based on the
// page size and max key size while initializing.
type RBTree struct {
	file     string
	pager    *pager.Pager
	pages    map[uint64]*page // node cache to avoid IO
	meta     *metadata        // metadata about tree structure
	root     *node            // current root node
	degree   uint16
	nodeSize uint16
}

func (tree *RBTree) Close() error {
	if tree.pager == nil {
		return nil
	}

	_ = tree.writeAll()
	err := tree.pager.Close()
	tree.pager = nil
	return err
}

func (tree *RBTree) pointer(rawPtr uint64) *pointer {
	return &pointer{
		raw:    rawPtr,
		pageId: rawPtr / uint64(tree.degree),
		index:  uint16(rawPtr % uint64(tree.degree)),
	}
}

func (tree *RBTree) page(id uint64) *page {
	return &page{
		dirty: true,
		id:    id,
		size:  tree.meta.pageSize,
		nodes: make([]*node, tree.degree),
	}
}

func (tree *RBTree) fetch(rawPtr uint64) (*node, error) {
	ptr := tree.pointer(rawPtr)

	if p, ok := tree.pages[ptr.pageId]; ok {
		return p.nodes[ptr.index], nil
	}

	p := tree.page(ptr.pageId)
	if err := tree.pager.Unmarshal(ptr.pageId, p); err != nil {
		return nil, err
	}

	p.dirty = false
	tree.pages[ptr.pageId] = p

	if len(p.nodes) <= int(ptr.index) {
		return nil, ErrInvalidPointer
	}
	return p.nodes[ptr.index], nil
}

func (tree *RBTree) alloc(n int) ([]*node, error) {
	topPtr := tree.pointer(tree.meta.top)
	requestedLastPtr := tree.pointer(tree.meta.top + uint64(n * int(tree.nodeSize)))

	if requestedLastPtr.pageId > topPtr.pageId {
		var err error
		_, err = tree.pager.Alloc(int(requestedLastPtr.pageId - topPtr.pageId))
		if err != nil {
			return nil, err
		}
	}

	nodes := make([]*node, n)
	for i := 0; i < n; i++ {
		n, err := tree.fetch(tree.meta.top + uint64(i * int(tree.nodeSize)))
		if err != nil {
			return nil, err
		}
		nodes[i] = n
	}

	return nodes, nil
}

func (tree *RBTree) open(opts *Options) error {
	if tree.pager.Count() == 0 {
		return tree.init(opts)
	}

	if err := tree.pager.Unmarshal(0, tree.meta); err != nil {
		return err
	}

	root, err := tree.fetch(tree.meta.rootPtr)
	if err != nil {
		return err
	}
	tree.root = root

	return nil
}

// init initializes a new B+ tree in the underlying file. allocates 2 pages
// (1 for meta + 1 for root) and initializes the instance. metadata and the
// root node are expected to be written to file during insertion.
func (tree *RBTree) init(opts *Options) error {
	_, err := tree.pager.Alloc(1)
	if err != nil {
		return err
	}

	tree.meta = &metadata{
		dirty:    true,

		pageSize:    opts.PageSize,
		nodeKeySize: opts.KeySize,
		top:         uint64(opts.PageSize),
		rootPtr:     0,
	}

	return nil
}

// writeAll writes all the nodes marked dirty to the underlying pager.
func (tree *RBTree) writeAll() error {
	if tree.pager.ReadOnly() {
		return nil
	}

	for _, p := range tree.pages {
		if !p.dirty {
			for _, n := range p.nodes {
				if n.dirty {
					p.dirty = true
					n.dirty = false
					break
				}
			}
		}

		if p.dirty {
			if err := tree.pager.Marshal(p.id, p); err != nil {
				return err
			}
			p.dirty = false
		}
	}

	return tree.writeMeta()
}

func (tree *RBTree) writeMeta() error {
	if tree.meta.dirty {
		err := tree.pager.Marshal(0, tree.meta)
		tree.meta.dirty = false
		return err
	}

	return nil
}
