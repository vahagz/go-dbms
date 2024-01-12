package index

import "go-dbms/pkg/types"

func (i *Index) Delete(values map[string]types.DataType, withPK bool) (int, error) {
	var pk [][]byte
	if withPK {
		pk = i.primary.key(values)
	}

	return i.tree.DelMem(i.key(values), pk)
}
