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

func (t *Table) CreateIndex(name *string, columns []string, opts IndexOptions) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !opts.Primary && t.meta.PrimaryKey == nil {
		return errors.New("first index must be primary")
	}
	if opts.Primary && t.meta.PrimaryKey != nil {
		return errors.New("primary index already created")
	}
	if opts.AutoIncrement {
		if len(columns) != 1 {
			return fmt.Errorf("auto increment supported only fofr single column indexes")
		}
	}
	if name != nil {
		if _, ok := t.indexes[*name]; ok {
			return fmt.Errorf("index with name:'%s' already exists", *name)
		}
	}

	keySize := 0
	columnsList := make([]*column.Column, 0, len(columns))
	for _, columnName := range columns {
		if col, ok := t.meta.ColumnsMap[columnName]; !ok {
			return fmt.Errorf("unknown column:'%s'", columnName)
		} else if !col.Meta.IsFixedSize() {
			return fmt.Errorf("column must be of fixed size")
		} else {
			keySize += col.Meta.Size()
			columnsList = append(columnsList, col)
		}
	}

	if opts.AutoIncrement {
		if !columnsList[0].Meta.IsNumeric() {
			return fmt.Errorf("auto increment is supported for numeric types")
		}
	}

	if name == nil {
		name = new(string)
		*name = strings.Join(columns, "_")
		for i := 1; i < 100; i++ {
			postfix := fmt.Sprintf("_%d", i)
			if _, ok := t.indexes[*name + postfix]; !ok {
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
		opts := t.indexes[*t.meta.PrimaryKey].Options()
		suffixSize = opts.MaxKeySize
		suffixCols = opts.KeyCols
	}

	indexOpts := &bptree.Options{
		KeyCols:       len(columns),
		MaxSuffixSize: suffixSize,
		SuffixCols:    suffixCols,
		MaxKeySize:    keySize,
		MaxValueSize:  allocator.PointerSize,
		Degree:        10,
		PageSize:      os.Getpagesize(),
		Uniq:          opts.Uniq,
		CacheSize:     10000,
	}

	tree, err := bptree.Open(t.indexPath(*name), indexOpts)
	if err != nil {
		return err
	}

	meta := &index.Meta{
		Name:    *name,
		Columns: columns,
		Uniq:    opts.Uniq,
		Options: indexOpts,
	}

	i := index.New(meta, t.df, tree, columnsList, opts.Uniq)
	t.indexes[*name] = i

	err = t.df.Scan(func(ptr allocator.Pointable, row []types.DataType) (bool, error) {
		return false, i.Insert(ptr, t.row2map(row))
	})
	if err != nil {
		i.Remove()
		return err
	}

	if opts.Primary {
		t.meta.PrimaryKey = name
	} else {
		i.SetPK(t.indexes[*t.meta.PrimaryKey])
	}

	t.meta.Indexes = append(t.meta.Indexes, meta)
	return t.writeMeta()
}
