// Package data implements an on-disk data file scheme that can store
// several columns with types. Columns can be of type int, string, bool
package data

import (
	"encoding/binary"
	"fmt"
	"sync"

	"go-dbms/pkg/column"
	"go-dbms/pkg/customerrors"
	"go-dbms/pkg/types"

	"github.com/pkg/errors"
	allocator "github.com/vahagz/disk-allocator/heap"
	"github.com/vahagz/disk-allocator/heap/cache"
	"github.com/vahagz/pager"
)

// bin is the byte order used for all marshals/unmarshals.
var bin = binary.BigEndian

// Open opens the named file as a data file and returns an instance
// DataFile for use. Use ":memory:" for an in-memory DataFile instance for quick
// testing setup. If nil options are provided, defaultOptions will be used.
func Open(fileName string, opts *Options) (*DataFile, error) {
	if len(opts.Columns) == 0 {
		return nil, errors.New("provide at leas 1 column")
	}
	if opts == nil {
		opts = &DefaultOptions
	}

	pagerFile := fmt.Sprintf("%s.dat", fileName)
	p, err := pager.Open(pagerFile, opts.PageSize, false, 0644)
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

	df := &DataFile{
		file:    pagerFile,
		mu:      &sync.RWMutex{},
		heap:    heap,
		columns: opts.Columns,
	}

	df.cache = cache.NewCache[*record](10000, df.newEmptyRecord)

	if err := df.open(opts); err != nil {
		_ = df.Close()
		return nil, err
	}

	return df, nil
}

// DataFile represents an on-disk df. Several records
// in the df are mapped to a single page in the file. 
type DataFile struct {
	file    string
	metaPtr allocator.Pointable

	// df state
	mu      *sync.RWMutex
	heap    *allocator.Allocator
	cache   *cache.Cache[*record] // records cache to avoid IO
	meta    *metadata             // metadata about df structure
	columns []*column.Column      // columns list of data
}

// Get fetches the record from the given pointer. Returns error if record not found.
func (df *DataFile) Get(ptr allocator.Pointable) []types.DataType {
	df.mu.RLock()
	defer df.mu.RUnlock()

	r := df.fetchN(ptr).Get()
	dataCopy := make([]types.DataType, len(r.data))
	copy(dataCopy, r.data)
	return dataCopy
}

// InsertRecord inserts the value into the df
// and returns page id where was inserted
func (df *DataFile) Insert(val []types.DataType) (allocator.Pointable, error) {
	ptr, err := df.InsertMem(val)
	if err != nil {
		return nil, err
	}

	return ptr, df.writeAll()
}

func (df *DataFile) InsertMem(val []types.DataType) (allocator.Pointable, error) {
	if len(val) != len(df.columns) {
		return nil, customerrors.ErrKeyTooLarge
	}

	df.mu.Lock()
	defer df.mu.Unlock()

	ptr, err := df.insert(val)
	if err != nil {
		return nil, errors.Wrap(err, "failed to insert new record")
	}
	return ptr, nil
}

// Update updates record value. If new value can't fit to current
// record place, new pointer will be allocated and set.
// Pointer where data stored will be returned.
func (df *DataFile) Update(ptr allocator.Pointable, values []types.DataType) (allocator.Pointable, error) {
	if ptr, err := df.UpdateMem(ptr, values); err != nil {
		return nil, err
	} else {
		return ptr, df.writeAll()
	}
}

func (df *DataFile) UpdateMem(ptr allocator.Pointable, values []types.DataType) (allocator.Pointable, error) {
	df.mu.Lock()
	defer df.mu.Unlock()

	ptr, err := df.update(ptr, values)
	if err != nil {
		return nil, errors.Wrap(err, "failed to update data")
	}
	return ptr, nil
}

// Delete marks pointer as 'free' for future reuse
func (df *DataFile) Delete(ptr allocator.Pointable) error {
	df.DeleteMem(ptr)
	return df.writeAll()
}

func (df *DataFile) DeleteMem(ptr allocator.Pointable) {
	df.mu.Lock()
	defer df.mu.Unlock()
	df.delete(ptr)
}

// Scan performs pointers scan starting from first pointer (next pointer after meta)
func (df *DataFile) Scan(scanFn func(ptr allocator.Pointable, row []types.DataType) (bool, error)) error {
	df.mu.RLock()
	defer df.mu.RUnlock()

	r := df.newEmptyRecord()
	return df.heap.Scan(df.metaPtr, func(ptr allocator.Pointable) (bool, error) {
		if ptr.IsFree() {
			return false, nil
		} else if err := ptr.Get(r); err != nil {
			return true, errors.Wrap(err, "failed to get pointer data")
		} else if stop, err := scanFn(ptr, r.data); err != nil {
			return true, err
		} else if stop {
			return true, nil
		}
		return false, nil
	})
}

// Close flushes any writes and closes the underlying pager.
func (df *DataFile) Close() error {
	df.mu.Lock()
	defer df.mu.Unlock()

	if df.heap == nil {
		return nil
	}

	_ = df.writeAll() // write if any nodes are pending
	err := df.heap.Close()
	df.heap = nil
	return err
}

// Pointer returns ptr with zero value attached to underlying pager
func (df *DataFile) Pointer() allocator.Pointable {
	return df.heap.Nil()
}

// Link attaches underlying pager to pointer
func (df *DataFile) Link(ptr allocator.Pointable) {
	df.heap.Link(ptr)
}

func (df *DataFile) update(ptr allocator.Pointable, values []types.DataType) (allocator.Pointable, error) {
	newRecord := df.newRecord(values)
	if newRecord.Size() <= ptr.Size() {
		p := df.fetchW(ptr)
		r := p.Get()
		*r = *newRecord
		p.Unlock()
		return ptr, nil
	}

	df.delete(ptr)
	ptr = df.heap.Alloc(newRecord.Size())
	df.cache.Add(ptr)
	return ptr, ptr.Set(newRecord)
}

func (df *DataFile) delete(ptr allocator.Pointable) {
	df.cache.Del(ptr)
	df.heap.Free(ptr)
}

// newrecord initializes an in-memory record and returns.
func (df *DataFile) newRecord(data []types.DataType) *record {
	return &record{
		dirty:   true,
		data:    data,
		columns: df.columns,
	}
}

func (df *DataFile) newEmptyRecord() *record {
	return &record{
		dirty:   true,
		data:    make([]types.DataType, 0),
		columns: df.columns,
	}
}

func (df *DataFile) insert(val []types.DataType) (allocator.Pointable, error) {
	r := df.newRecord(val)
	ptr := df.heap.Alloc(r.Size())
	return ptr, ptr.Set(r)
}

// fetch returns the record from given pointer. underlying file is accessed
// only if the record doesn't exist in cache.
func (df *DataFile) fetchF(ptr allocator.Pointable, flag cache.LOCKMODE) cache.Pointable[*record] {
	nPtr := df.cache.GetF(ptr, flag)
	if nPtr != nil {
		return nPtr
	}

	r := df.newEmptyRecord()
	if err := ptr.Get(r); err != nil {
		panic(errors.Wrap(err, "failed to get record data from pointer"))
	}

	r.Dirty(false)
	return df.cache.AddF(ptr, flag)
}

func (df *DataFile) fetchR(ptr allocator.Pointable) cache.Pointable[*record] {
	return df.fetchF(ptr, cache.READ)
}

func (df *DataFile) fetchW(ptr allocator.Pointable) cache.Pointable[*record] {
	return df.fetchF(ptr, cache.WRITE)
}

func (df *DataFile) fetchN(ptr allocator.Pointable) cache.Pointable[*record] {
	return df.fetchF(ptr, cache.NONE)
}

// open opens the df stored on disk using the pager. If the pager
// has no pages, a new df will be initialized.
func (df *DataFile) open(opts *Options) error {
	df.metaPtr = df.heap.FirstPointer(metadataSize)
	if df.heap.Size() == df.metaPtr.Addr() - allocator.PointerMetaSize {
		// initialize a new df
		return df.init(opts)
	}

	df.meta = &metadata{}
	if err := df.metaPtr.Get(df.meta); err != nil {
		return errors.Wrap(err, "failed to read meta while opening datafile")
	}

	// verify metadata
	if df.meta.version != version {
		return fmt.Errorf("incompatible version %#x (expected: %#x)", df.meta.version, version)
	}

	return nil
}

// init initializes a new df in the underlying file. allocates 1 page
// for meta) and initializes the instance. metadata is expected to 
// be written to file during insertion.
func (df *DataFile) init(opts *Options) error {
	df.meta = &metadata{
		dirty:   true,
		version: version,
		flags:   0,
		pageSize: uint16(opts.PageSize),
	}

	df.metaPtr = df.heap.Alloc(metadataSize)

	return errors.Wrap(df.metaPtr.Set(df.meta), "failed to write meta after init")
}

// writeAll writes all the records marked dirty to the underlying pager.
func (df *DataFile) writeAll() error {
	df.cache.Flush()
	return df.writeMeta()
}

func (df *DataFile) writeMeta() error {
	if df.meta.dirty {
		return df.metaPtr.Set(df.meta)
	}
	return nil
}
