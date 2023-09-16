// Package data implements an on-disk data file scheme that can store
// several columns with types. Columns can be of type int, string, bool
package data

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"go-dbms/pkg/index"
	pager "go-dbms/pkg/slotted_pager"
	"go-dbms/pkg/types"
)

// bin is the byte order used for all marshals/unmarshals.
var bin = binary.BigEndian

// Open opens the named file as a data file and returns an instance
// DataFile for use. Use ":memory:" for an in-memory DataFile instance for quick
// testing setup. If nil options are provided, defaultOptions will be used.
func Open(fileName string, opts *Options) (*DataFile, error) {
	if opts == nil {
		opts = &defaultOptions
	}

	p, err := pager.Open(fileName, opts.PageSize, opts.ReadOnly, opts.FileMode)
	if err != nil {
		return nil, err
	}

	df := &DataFile{
		mu:    &sync.RWMutex{},
		file:  fileName,
		pager: p,
		pages: map[int]*pager.Page[*record]{},
		meta:  &metadata{},
	}

	// initialize the df if new or open the existing df and load
	// root node.
	if err := df.open(*opts); err != nil {
		_ = df.Close()
		return nil, err
	}

	return df, nil
}

// DataFile represents an on-disk df. Several records
// in the df are mapped to a single page in the file. 
type DataFile struct {
	file string

	// df state
	mu      *sync.RWMutex
	pager   *pager.Pager
	pages   map[int]*pager.Page[*record] // page cache to avoid IO
	meta    *metadata                    // metadata about df structure
}

// Get fetches the record from the given pointer. Returns error if record not found.
func (df *DataFile) GetPage(id int) ([][]types.DataType, error) {
	if id <= 0 {
		return nil, index.ErrEmptyKey
	}

	df.mu.RLock()
	defer df.mu.RUnlock()

	p, err := df.fetch(id)
	if err != nil {
		return nil, err
	}

	result := make([][]types.DataType, len(p.Slots))
	for i, slot := range p.Slots {
		result[i] = slot.data
	}
	return result, nil
}

// Put puts the value into the df and returns its id
func (df *DataFile) InsertSlot(val []types.DataType) (int, error) {
	id, err := df.InsertSlotMem(val)
	if err != nil {
		return 0, err
	}

	return id, df.writeAll()
}

func (df *DataFile) InsertSlotMem(val []types.DataType) (int, error) {
	if len(val) != len(df.meta.columns) {
		return 0, index.ErrKeyTooLarge
	}

	df.mu.Lock()
	defer df.mu.Unlock()

	p, _, err := df.put(val)
	if err != nil {
		return 0, err
	}

	return p.Id, nil
}

// Del removes all slots from page. If the
// page does not exist, returns error.
func (df *DataFile) DeletePage(id int) error {
	df.mu.Lock()
	defer df.mu.Unlock()

	p, err := df.fetch(id)
	if err != nil {
		return err
	}

	p.ClearSlots()
	p.Dirty = true
	df.meta.freeList[id] = p.FreeSpace

	return nil
}

func (df *DataFile) UpdatePage(id int, values [][]types.DataType) error {
	df.mu.Lock()
	defer df.mu.Unlock()

	p, err := df.fetch(id)
	if err != nil {
		return err
	}

	p.Dirty = true
	p.ClearSlots()
	for _, data := range values {
		r := newRecord(id, df.meta)
		r.data = data
		if _, err := p.AddSlot(r); err != nil {
			return err
		}
	}
	df.meta.freeList[id] = p.FreeSpace

	return nil
}

// Scan performs an index scan starting at the given key. Each entry will be
// passed to the scanFn. If the key is zero valued (nil or len=0), then the
// left/right leaf key will be used as the starting key. Scan continues until
// the right most leaf node is reached or the scanFn returns 'true' indicating
// to stop the scan. If reverse=true, scan starts at the right most node and
// executes in descending order of keys.
// func (df *DataFile) Scan(key []byte, reverse bool, scanFn func(key []byte, v uint64) bool) error {
// 	df.mu.RLock()
// 	defer df.mu.RUnlock()

// 	if df.meta.size == 0 {
// 		return nil
// 	}

// 	var err error
// 	var beginAt *node
// 	idx := 0

// 	if len(key) == 0 {
// 		// No explicit key provided by user, find the a leaf-node based on
// 		// scan direction and start there.
// 		if !reverse {
// 			beginAt, err = df.leftLeaf(df.root)
// 			idx = 0
// 		} else {
// 			beginAt, err = df.rightLeaf(df.root)
// 			idx = len(beginAt.entries) - 1
// 		}
// 	} else {
// 		// we have a specific key to start at. find the node containing the
// 		// key and start the scan there.
// 		beginAt, idx, _, err = df.searchRec(df.root, key)
// 	}

// 	if err != nil {
// 		return err
// 	}

// 	// starting at found leaf node, follow the 'next' pointer until.
// 	var nextNode int

// 	for beginAt != nil {
// 		if !reverse {
// 			for i := idx; i < len(beginAt.entries); i++ {
// 				e := beginAt.entries[i]
// 				if scanFn(e.key, e.val) {
// 					break
// 				}
// 			}
// 			nextNode = beginAt.next
// 		} else {
// 			for i := idx; i >= 0; i-- {
// 				e := beginAt.entries[i]
// 				if scanFn(e.key, e.val) {
// 					break
// 				}
// 			}
// 			nextNode = beginAt.prev
// 		}
// 		idx = 0

// 		if nextNode == 0 {
// 			break
// 		}

// 		beginAt, err = df.fetch(nextNode)
// 		if err != nil {
// 			return err
// 		}
// 	}

// 	return nil
// }

// Size returns the number of entries in the entire df
func (df *DataFile) Size() int64 { return int64(df.meta.size) }

// Close flushes any writes and closes the underlying pager.
func (df *DataFile) Close() error {
	df.mu.Lock()
	defer df.mu.Unlock()

	if df.pager == nil {
		return nil
	}

	_ = df.writeAll() // write if any nodes are pending
	err := df.pager.Close()
	df.pager = nil
	return err
}

func (df *DataFile) String() string {
	return fmt.Sprintf(
		"DataFile{file='%s', size=%d}",
		df.file, df.Size(),
	)
}

func (df *DataFile) put(val []types.DataType) (*pager.Page[*record], int, error) {
	r := newRecord(0, df.meta)
	r.data = val

	page, err := df.alloc(r.Size())
	if err != nil {
		return nil, -1, err
	}

	r.id = page.Id
	index, err := page.AddSlot(r)
	if err != nil {
		return nil, -1, err
	}

	df.meta.freeList[page.Id] = page.FreeSpace
	
	df.meta.size++
	df.meta.dirty = true
	df.pages[page.Id] = page

	return page, index, nil
}

// fetch returns the record from given pointer. underlying file is accessed
// only if the record doesn't exist in cache.
func (df *DataFile) fetch(id int) (*pager.Page[*record], error) {
	page, found := df.pages[id]
	if found {
		if page.Flags & PAGE_FLAG_DELETED != 0 {
			return nil, errors.New("page deleted")
		}
		return page, nil
	}

	page = pager.NewPage(id, int(df.meta.pageSz), newRecord(id, df.meta))
	if err := df.pager.Unmarshal(id, page); err != nil {
		return nil, err
	}

	if page.Flags & PAGE_FLAG_DELETED != 0 {
		return nil, errors.New("page deleted")
	}

	page.Dirty = false
	df.pages[page.Id] = page

	return page, nil
}

// alloc allocates page required to store data. alloc will reuse
// pages from free-list if available.
func (df *DataFile) alloc(minSize int) (*pager.Page[*record], error) {
	// check if there are enough free pages from the freelist
	pid := 0
	freeSpace := 0
	for id, fs := range df.meta.freeList {
		if (pid == 0 && fs >= minSize) || (fs >= minSize && fs < freeSpace) {
			pid = id
			freeSpace = fs
		}
	}

	// there could be case when there is no enogh space in any page from freeList
	if pid == 0 {
		pid, err := df.pager.Alloc(1)
		if err != nil {
			return nil, err
		}

		page := pager.NewPage(pid, int(df.meta.pageSz), newRecord(pid, df.meta))
		return page, nil
	}

	page := pager.NewPage(pid, int(df.meta.pageSz), newRecord(pid, df.meta))
	return page, df.pager.Unmarshal(pid, page)
}

// open opens the df stored on disk using the pager. If the pager
// has no pages, a new df will be initialized.
func (df *DataFile) open(opts Options) error {
	if df.pager.Count() == 0 {
		// pager has no pages. initialize a new index.
		err := df.init(opts)
		if err != nil {
			return err
		}

		return df.pager.Marshal(0, df.meta)
	}

	// we are opening an initialized index file. read page 0 as metadata.
	if err := df.pager.Unmarshal(0, df.meta); err != nil {
		return err
	}

	// verify metadata
	if df.meta.version != version {
		return fmt.Errorf("incompatible version %#x (expected: %#x)", df.meta.version, version)
	} else if df.pager.PageSize() != int(df.meta.pageSz) {
		return errors.New("page size in meta does not match pager")
	}

	return nil
}

// init initializes a new df in the underlying file. allocates 1 page
// for meta) and initializes the instance. metadata is expected to 
// be written to file during insertion.
func (df *DataFile) init(opts Options) error {
	_, err := df.pager.Alloc(1 + opts.PreAlloc)
	if err != nil {
		return err
	}

	columns := []column{}
	for name, typeCode := range opts.Columns {
		columns = append(columns, column{
			name: name,
			typ: typeCode,
		})
	}

	df.meta = &metadata{
		dirty:   true,
		version: version,
		flags:   0,
		size:    0,
		pageSz:  uint32(df.pager.PageSize()),
		columns: columns,
	}

	df.meta.freeList = make(map[int]int, opts.PreAlloc)
	for i := 0; i < opts.PreAlloc; i++ {
		df.meta.freeList[i + 1] = int(df.meta.pageSz) // +1 since first page reserved
	}

	return nil
}

// writeAll writes all the records marked dirty to the underlying pager.
func (df *DataFile) writeAll() error {
	if df.pager.ReadOnly() {
		return nil
	}

	for _, p := range df.pages {
		if p.Dirty {
			if err := df.pager.Marshal(p.Id, p); err != nil {
				return err
			}
			p.Dirty = false
		}
	}

	return df.writeMeta()
}

func (df *DataFile) writeMeta() error {
	if df.meta.dirty {
		err := df.pager.Marshal(0, df.meta)
		df.meta.dirty = false
		return err
	}

	return nil
}

// allocSeq finds a subset of size 'n' in 'free' that is sequential.
// Returns the first int in the sequence the set after removing the
// subset.
func allocSeq(free []int, n int) (id int, remaining []int) {
	if len(free) <= n {
		return -1, free
	} else if n == 1 {
		return free[0], free[1:]
	}

	i, j := 0, 0
	for ; i < len(free); i++ {
		j = i + (n - 1)
		if j < len(free) && free[j] == free[i]+(n-1) {
			break
		}
	}

	if i >= len(free) || j >= len(free) {
		return -1, free
	}

	id = free[i]
	free = append(free[:i], free[j+1:]...)
	return id, free
}
