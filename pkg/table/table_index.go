package table

import (
	"fmt"
	"os"
	"strings"

	"go-dbms/pkg/column"
	"go-dbms/pkg/index"
	"go-dbms/pkg/types"

	"github.com/pkg/errors"
	"github.com/vahagz/bptree"
	allocator "github.com/vahagz/disk-allocator/heap"
)

func (t *Table) CreateIndex(name *string, opts *index.IndexOptions) error {
	t.Mu.Lock()
	defer t.Mu.Unlock()

	if !opts.Primary && t.Meta.PrimaryKey == "" {
		return errors.New("first index must be primary")
	}
	if opts.Primary && t.Meta.PrimaryKey != "" {
		return errors.New("primary index already created")
	}
	if name != nil {
		if _, ok := t.Indexes[*name]; ok {
			return fmt.Errorf("index with name:'%s' already exists", *name)
		}
	}

	keySize := 0
	columnsList := make([]*column.Column, 0, len(opts.Columns))
	for _, columnName := range opts.Columns {
		if col, ok := t.Meta.ColumnsMap[columnName]; !ok {
			return fmt.Errorf("unknown column:'%s'", columnName)
		} else if !col.Meta.IsFixedSize() {
			return fmt.Errorf("column must be of fixed size")
		} else {
			keySize += col.Meta.Size()
			columnsList = append(columnsList, col)
		}
	}

	if name == nil {
		name = new(string)
		*name = strings.Join(opts.Columns, "_")
		for i := 1; i < 100; i++ {
			postfix := fmt.Sprintf("_%d", i)
			if _, ok := t.Indexes[*name + postfix]; !ok {
				*name += postfix
				break
			}
		}
	}

	if opts.Primary {
		opts.Uniq = true
	}

	suffixSize := 0
	suffixCols := 0
	if !opts.Primary {
		opts := t.Indexes[t.Meta.PrimaryKey].Options()
		suffixSize = opts.MaxKeySize
		suffixCols = opts.KeyCols
	}

	indexOpts := &bptree.Options{
		KeyCols:       len(opts.Columns),
		MaxSuffixSize: suffixSize,
		SuffixCols:    suffixCols,
		MaxKeySize:    keySize,
		MaxValueSize:  allocator.PointerSize,
		Degree:        500,
		PageSize:      os.Getpagesize(),
		Uniq:          opts.Uniq,
		CacheSize:     10000,
	}

	tree, err := bptree.Open(t.indexPath(*name), indexOpts)
	if err != nil {
		return err
	}

	Meta := &index.Meta{
		Name:    *name,
		Columns: opts.Columns,
		Uniq:    opts.Uniq,
		Options: indexOpts,
	}

	i := index.New(Meta, t.DF, tree, columnsList, opts.Uniq)
	t.Indexes[*name] = i

	err = t.DF.Scan(func(ptr allocator.Pointable, row []types.DataType) (bool, error) {
		return false, i.Insert(ptr, t.Row2map(row))
	})
	if err != nil {
		i.Remove()
		return err
	}

	if opts.Primary {
		t.Meta.PrimaryKey = *name
	} else {
		i.SetPK(t.Indexes[t.Meta.PrimaryKey])
	}

	t.Meta.Indexes = append(t.Meta.Indexes, Meta)
	t.writeMeta()
	return nil
}
