package main

import (
	"encoding/json"
	"fmt"
	"go-dbms/pkg/column"
	"go-dbms/pkg/table"
	"go-dbms/pkg/types"
	r "math/rand"
	"os"
	"path"
	"time"

	"github.com/sirupsen/logrus"
)

var seed = time.Now().Unix()
// var seed int64 = 1702483180
var rand = r.New(r.NewSource(seed))

// func main() {
// 	configs := config.New()
// 	svcs := services.New()
// 	err := server.Start(configs.ServerConfig, svcs)
// 	fmt.Printf(err)
// }




func main() {
	logrus.SetLevel(logrus.DebugLevel)

	dir, _ := os.Getwd()
	tablePath := path.Join(dir, "test", "table")
	options := &table.Options{
		Columns: []*column.Column{
			column.New("id",        types.Meta(types.TYPE_INTEGER, true, 4)),
			column.New("firstname", types.Meta(types.TYPE_VARCHAR, 32)),
			column.New("lastname",  types.Meta(types.TYPE_VARCHAR, 32)),
		},
	}

	t, err := table.Open(tablePath, options)
	if err != nil {
		logrus.Fatal(err)
	}

	var getDuration time.Duration
	var insertDuration time.Duration
	var deleteDuration time.Duration
	start := time.Now()
	exitFunc := func() {
		fmt.Println()
		fmt.Println("INSERT DURATION =>", insertDuration)
		fmt.Println("DELETE DURATION =>", deleteDuration)
		fmt.Println("GET DURATION =>", getDuration)
		fmt.Println("TOTAL DURATION =>", time.Since(start))
		fmt.Println("SEED =>", seed)
		if err := t.Close(); err != nil {
			logrus.Error(err)
		}
	}
	logrus.RegisterExitHandler(exitFunc)
	defer exitFunc()

	// err = t.CreateIndex(nil, []string{"id"}, table.IndexOptions{ Primary: true })
	// if err != nil {
	// 	logrus.Fatal(err)
	// }

	// err = t.CreateIndex(nil, []string{"firstname","lastname"}, table.IndexOptions{})
	// if err != nil {
	// 	logrus.Fatal(err)
	// }

	// ids      := []int{5,6,4,5,7,2,1,9}
	// names    := []string{"Vahag",     "Sergey",    "Bagrat",   "Mery"}
	// surnames := []string{"Zargaryan", "Voskanyan", "Galstyan", "Sargsyan"}
	// for _, id := range ids {
	// 	_, err := t.Insert(map[string]types.DataType{
	// 		"id":        types.Type(t.ColumnsMap()["id"].Meta).Set(id),
	// 		"firstname": types.Type(t.ColumnsMap()["firstname"].Meta).Set(names[rand.Int31n(4)]),
	// 		"lastname":  types.Type(t.ColumnsMap()["lastname"].Meta).Set(surnames[rand.Int31n(4)]),
	// 	})
	// 	if err != nil {
	// 		fmt.Println(id, err)
	// 	}
	// }

	fmt.Println("id_1")
	err = t.FullScanByIndex("id_1", false, func(row map[string]types.DataType) (bool, error) {
		printData(t.Columns(), []map[string]types.DataType{row})
		return false, nil
	})
	if err != nil {
		logrus.Fatal(err)
	}

	fmt.Println("firstname_lastname_1")
	err = t.FullScanByIndex("firstname_lastname_1", false, func(row map[string]types.DataType) (bool, error) {
		printData(t.Columns(), []map[string]types.DataType{row})
		return false, nil
	})
	if err != nil {
		logrus.Fatal(err)
	}

	fmt.Println("=======================")
	records, err := t.FindByIndex(
		// "id_1",
		"firstname_lastname_1",
		">",
		map[string]types.DataType{
			// "id": types.Type(t.ColumnsMap()["id"].Meta).Set(5),
			"firstname": types.Type(t.ColumnsMap()["firstname"].Meta).Set("Sergey"),
			// "lastname":  types.Type(t.ColumnsMap()["lastname"].Meta).Set("Sargsyan"),
		},
	)
	if err != nil {
		logrus.Fatal(err)
	}
	printData(t.Columns(), records)

	// for i := 0; i < 10; i++ {
	// 	record, err := t.FindByIndex("id_1", false, map[string]types.DataType{
	// 		"id": types.Type(types.TYPE_INT, t.ColumnsMap()["id"].Meta).Set(i),
	// 	})
	// 	if err != nil {
	// 		logrus.Error(err)
	// 		continue
	// 	}
	// 	printData(t.Columns(), record)
	// }
}

// func main() {
// 	logrus.SetLevel(logrus.DebugLevel)
// 	pwd, _ := os.Getwd()
// 	// os.Remove(path.Join(pwd, "test", "bptree.idx"))
// 	// os.Remove(path.Join(pwd, "test", "bptree_freelist.bin"))

// 	// bptreeFile := path.Join(pwd, "test", "bptree")

// 	// tree, err := bptree.Open(bptreeFile, &bptree.Options{
// 	// 	PageSize:     os.Getpagesize(),
// 	// 	MaxKeySize:   4,
// 	// 	MaxValueSize: 1,
// 	// 	Degree:       10,
// 	// 	KeyCols:      1,
// 	// 	Uniq:         false,
// 	// })
// 	// if err != nil {
// 	// 	logrus.Fatal(err)
// 	// }

// 	dataFile := path.Join(pwd, "test", "data.bin")

// 	df, err := data.Open(dataFile, &data.Options{
// 		PageSize: os.Getpagesize(),
// 		Columns: []*column.Column{
// 			column.New("id",        types.Meta(types.TYPE_INTEGER, true, 4)),
// 			column.New("firstname", types.Meta(types.TYPE_VARCHAR, 32)),
// 			column.New("lastname",  types.Meta(types.TYPE_VARCHAR, 32)),
// 		},
// 	})
// 	if err != nil {
// 		logrus.Fatal(err)
// 	}

// 	var getDuration time.Duration
// 	var insertDuration time.Duration
// 	var deleteDuration time.Duration
// 	start := time.Now()
// 	exitFunc := func() {
// 		fmt.Println()
// 		fmt.Println("INSERT DURATION =>", insertDuration)
// 		fmt.Println("DELETE DURATION =>", deleteDuration)
// 		fmt.Println("GET DURATION =>", getDuration)
// 		fmt.Println("TOTAL DURATION =>", time.Since(start))
// 		fmt.Println("SEED =>", seed)
// 		if err := df.Close(); err != nil {
// 			logrus.Error(err)
// 		}
// 		// if err := tree.Close(); err != nil {
// 		// 	logrus.Error(err)
// 		// }
// 	}
// 	logrus.RegisterExitHandler(exitFunc)
// 	defer exitFunc()


// 	row := []types.DataType{
// 		types.Type(types.Meta(types.TYPE_INTEGER, true, 4)).Set(5),
// 		types.Type(types.Meta(types.TYPE_VARCHAR, 32)).Set(randomString(7)),
// 		types.Type(types.Meta(types.TYPE_VARCHAR, 32)).Set(randomString(13)),
// 	}
// 	ptr, err := df.Insert(row)
// 	if err != nil {
// 		logrus.Fatal(err)
// 	} else {
// 		fmt.Println("inserted =>", ptr, row[0].Value(), row[1].Value(), row[2].Value())
// 	}

// 	df.Scan(func(ptr allocator.Pointable, row []types.DataType) (bool, error) {
// 		fmt.Println(ptr, row[0].Value(), row[1].Value(), row[2].Value())
// 		return false, nil
// 	})

// 	fmt.Println()
// 	df.Delete(ptr)

// 	df.Scan(func(ptr allocator.Pointable, row []types.DataType) (bool, error) {
// 		fmt.Println(ptr, row[0].Value(), row[1].Value(), row[2].Value())
// 		return false, nil
// 	})

// 	// list := make([][][]byte, 0, 1000)
// 	// // tree.PrepareSpace(2*1024)
// 	// for i := 0; i < 10; i++ {
// 	// 	key := make([]byte, 4)
// 	// 	binary.BigEndian.PutUint32(key, uint32(rand.Int31()))
// 	// 	list = append(list, [][]byte{key})
// 	// 	err = tree.PutMem(list[i], []byte{list[i][0][1]}, &bptree.PutOptions{
// 	// 		Update: false,
// 	// 	})
// 	// 	if err != nil {
// 	// 		logrus.Fatal(err)
// 	// 	}

// 	// 	if val, err := tree.Get(list[i]); err != nil {
// 	// 		fmt.Println(i, list[i], val, err)
// 	// 	}
// 	// }
// 	// if err := tree.WriteAll(); err != nil {
// 	// 	logrus.Fatal(err)
// 	// }
// 	// insertDuration = time.Since(start)

// 	// keys := [][]byte{
// 	// 	{4,5,6,7},
// 	// 	{3,4,5,6},
// 	// 	{6,7,8,9},
// 	// 	{8,9,10,11},
// 	// 	{1,2,3,4},
// 	// 	{2,3,4,5},
// 	// 	{2,3,4,5},
// 	// 	{1,2,3,4},
// 	// 	{10,11,12,13},
// 	// 	{1,2,3,4},
// 	// 	{8,9,10,11},
// 	// 	{1,2,3,4},
// 	// 	{1,2,3,4},
// 	// 	{8,9,10,11},
// 	// 	{8,9,10,11},
// 	// 	{2,3,4,5},
// 	// 	{10,11,12,13},
// 	// }
// 	// _ = keys
// 	// for i := range keys {
// 	// 	_, err = tree.Put([][]byte{keys[i]}, []byte{byte(i)}, bptree.PutOptions{
// 	// 		Update: false,
// 	// 	})
// 	// 	if err != nil {
// 	// 		logrus.Fatal(err)
// 	// 	}
// 	// 	// fmt.Println(tree.Get([][]byte{keys[i]}))
// 	// }

// 	// fmt.Println(tree.DelMem([][]byte{{2,3,4,5}}))
// 	// fmt.Println(tree.DelMem([][]byte{{3,4,5,6}}))
// 	// fmt.Println(tree.DelMem([][]byte{{1,2,3,4}}))
// 	// fmt.Println(tree.DelMem([][]byte{{7,8,9,10}}))
// 	// fmt.Println(tree.DelMem([][]byte{{6,7,8,9}}))
// 	// fmt.Println(tree.DelMem([][]byte{{11,12,13,14}}))
// 	// fmt.Println(tree.DelMem([][]byte{{12,13,14,15}}))
// 	// fmt.Println(tree.DelMem([][]byte{{5,6,7,8}}))
// 	// fmt.Println(tree.DelMem([][]byte{{9,10,11,12}}))
// 	// if err := tree.WriteAll(); err != nil {
// 	// 	logrus.Fatal(err)
// 	// }
	
// 	// tree.PrepareSpace(30*1024*1024)
// 	// n := 1000

// 	// for j := 0; j < 1; j++ {
// 	// 	fmt.Println(j)
// 	// 	seed = time.Now().Unix()
// 	// 	rand = r.New(r.NewSource(seed))

// 	// 	list := make([][]byte, 0, n)
// 	// 	insertStart := time.Now()
// 	// 	for i := 0; i < n; i++ {
// 	// 		key := make([]byte, 4)
// 	// 		binary.BigEndian.PutUint32(key, uint32(rand.Int()))
// 	// 		list = append(list, key)
// 	// 		_, err = tree.PutMem([][]byte{list[i]}, []byte{byte(list[i][1])}, bptree.PutOptions{
// 	// 			Update: false,
// 	// 		})
// 	// 		if err != nil {
// 	// 			logrus.Fatal(err)
// 	// 		}

// 	// 		if i + 1 % 100000 == 0 {
// 	// 			if ok := tree.CheckConsistency(list); !ok {
// 	// 				fmt.Println(i)
// 	// 				panic("consistency check failed (while inserting)")
// 	// 			}
// 	// 		}
// 	// 	}
// 	// 	if err := tree.WriteAll(); err != nil {
// 	// 		logrus.Fatal(err)
// 	// 	}
// 	// 	insertDuration = time.Since(insertStart)

// 	// 	if ok := tree.CheckConsistency(list[:]); !ok {
// 	// 		panic("consistency check failed (after insert)")
// 	// 	}

// 	// 	deleteStart := time.Now()
// 	// 	for i := 0; i < 300; i++ {
// 	// 		_ = tree.DelMem([][]byte{list[i]})

// 	// 		if i + 1 % 100000 == 0 {
// 	// 			if ok := tree.CheckConsistency(list[i+1:]); !ok {
// 	// 				fmt.Println(i)
// 	// 				panic("consistency check failed (while deleting)")
// 	// 			}
// 	// 		}
// 	// 	}
// 	// 	if err := tree.WriteAll(); err != nil {
// 	// 		logrus.Fatal(err)
// 	// 	}
// 	// 	deleteDuration = time.Since(deleteStart)

// 	// 	fmt.Println("insert =>", insertDuration)
// 	// 	fmt.Println("delete =>", deleteDuration)
// 	// }

// 	// i := 1
// 	// err = tree.Scan(nil, bptree.ScanOptions{
// 	// 	Reverse: false,
// 	// 	Strict:  false,
// 	// }, func(key [][]byte, val []byte) (bool, error) {
// 	// 	fmt.Println(i, key, val)
// 	// 	i++
// 	// 	return false, nil
// 	// })
// 	// if err != nil {
// 	// 	logrus.Fatal(err)
// 	// }
// 	// tree.Print()
// 	// fmt.Println(tree.Count())


// 	// vals, err := tree.Del([][]byte{{6,7,8}})
// 	// if err != nil {
// 	// 	logrus.Fatal(err)
// 	// }
// 	// fmt.Println(vals)

// 	// err = tree.Scan(nil, false, true, func(key [][]byte, val []byte) (bool, error) {
// 	// 	fmt.Printf("key -> %v, val -> %v\n", key, val)
// 	// 	return false, nil
// 	// })
// 	// if err != nil {
// 	// 	logrus.Fatal(err)
// 	// }
// 	// fmt.Println(tree.Count())

// 	// if counter != 1000 {
// 	// 	bin, _ := json.Marshal(list)
// 	// 	fmt.Println(string(bin))
// 	// }
// 	// fmt.Println(counter)




// 	// pagerFile := path.Join(pwd, "test", "heap.dat")
// 	// p, err := pager.Open(pagerFile, os.Getpagesize(), false, 0644)
// 	// if err != nil {
// 	// 	logrus.Fatal(err)
// 	// }

// 	// allocatorFile := path.Join(pwd, "test", "freelist")
// 	// // os.Remove(allocatorFile)
// 	// a, err := allocator.Open(
// 	// 	allocatorFile,
// 	// 	&allocator.Options{
// 	// 		TargetPageSize: uint16(os.Getpagesize()),
// 	// 		TreePageSize:   uint16(os.Getpagesize()),
// 	// 		Pager:          p,
// 	// 	},
// 	// )
// 	// if err != nil {
// 	// 	logrus.Fatal(err)
// 	// }

// 	// c := cache.NewCache[binaryMarshalerUnmarshaler](3)

// 	// start := time.Now()
// 	// exitFunc := func() {
// 	// 	fmt.Println("\nTOTAL DURATION =>", time.Since(start))
// 	// 	if err := a.Close(); err != nil {
// 	// 		logrus.Error(err)
// 	// 	}
// 	// }
// 	// logrus.RegisterExitHandler(exitFunc)
// 	// defer exitFunc()


// 	// val := &binaryMarshalerUnmarshaler{Item: []int{rand.Intn(10),rand.Intn(10),rand.Intn(10)}}
// 	// ptr, err := a.Alloc(val.Size())
// 	// if err != nil {
// 	// 	logrus.Fatal(err)
// 	// }
// 	// cPtr := c.Add(ptr)
// 	// cPtr.Lock().Set(val).Flush()

// 	// for i := 0; i < 5; i++ {
// 	// 	val := &binaryMarshalerUnmarshaler{Item: []int{rand.Intn(10),rand.Intn(10),rand.Intn(10)}}
// 	// 	ptr, err := a.Alloc(val.Size())
// 	// 	if err != nil {
// 	// 		logrus.Fatal(err)
// 	// 	}
// 	// 	cPtr := c.Add(ptr)
// 	// 	cPtr.Lock().Set(val).Flush().Unlock()

// 	// 	fmt.Println("=====================================")
// 	// 	fmt.Println(c.Get(a.Pointer(23, 16)))
// 	// 	fmt.Println(c.Get(a.Pointer(49, 16)))
// 	// 	fmt.Println(c.Get(a.Pointer(75, 16)))
// 	// 	fmt.Println(c.Get(a.Pointer(101, 16)))
// 	// 	fmt.Println(c.Get(a.Pointer(127, 16)))
// 	// 	fmt.Println(c.Get(a.Pointer(153, 16)))
// 	// }

// 	// cPtr.Unlock()
// 	// fmt.Println("=====================================")
// 	// fmt.Println(c.Get(a.Pointer(23, 16)))
// 	// fmt.Println(c.Get(a.Pointer(49, 16)))
// 	// fmt.Println(c.Get(a.Pointer(75, 16)))
// 	// fmt.Println(c.Get(a.Pointer(101, 16)))
// 	// fmt.Println(c.Get(a.Pointer(127, 16)))
// 	// fmt.Println(c.Get(a.Pointer(153, 16)))

// 	// _ = c
// 	// val := &binaryMarshalerUnmarshaler{}
// 	// err = a.Pointer(153, 16).Get(val)
// 	// fmt.Println(val, err)


// 	// cPtr := c.Add(a.Pointer(49, 16))
// 	// cPtr.RLock()
// 	// go func() {
// 	// 	time.Sleep(5*time.Second)
// 	// 	fmt.Println("RUnlock", cPtr.Get())
// 	// 	cPtr.RUnlock()
// 	// }()

// 	// cPtr.Lock()
// 	// fmt.Println("Lock", cPtr.Get())
// 	// cPtr.Unlock()



// 	// ptr1, err := a.Alloc(4050)
// 	// if err != nil {
// 	// 	logrus.Fatal(err)
// 	// }
// 	// fmt.Println("alloc 1", ptr1)

// 	// ptr2, err := a.Alloc(100)
// 	// if err != nil {
// 	// 	logrus.Fatal(err)
// 	// }
// 	// fmt.Println("alloc 2", ptr2)

// 	// if err := a.Free(ptr2); err != nil {
// 	// 	logrus.Fatal(err)
// 	// }
// 	// fmt.Println("free 2")
	
// 	// ptr3, err := a.Alloc(2000)
// 	// if err != nil {
// 	// 	logrus.Fatal(err)
// 	// }
// 	// fmt.Println("alloc 3", ptr3)


// 	// item1 := &binaryMarshalerUnmarshaler{
// 	// 	map[string][]int{"dsadsads":{1,2,3}},
// 	// }
// 	// pt, err := a.Alloc(uint32(item1.Size()))
// 	// if err != nil {
// 	// 	logrus.Fatal(err)
// 	// }
// 	// ptr := allocator.Wrap[binaryMarshalerUnmarshaler](pt)
// 	// fmt.Println(ptr)
// 	// ptr.Set(item1)
// 	// fmt.Println(ptr.Get())


// 	// ptr := allocator.Wrap[binaryMarshalerUnmarshaler](a.Pointer(77, uint32(item1.Size())))
// 	// fmt.Println(ptr)
// 	// fmt.Println(ptr.Get())


// 	// fmt.Println(ptr)
// 	// if err := a.Free(ptr); err != nil {
// 	// 	logrus.Fatal(err)
// 	// }


// 	// if ptr, err := a.Alloc(1024 * 1024); err != nil {
// 	// 	logrus.Fatal(err)
// 	// } else if err := a.Free(ptr); err != nil {
// 	// 	logrus.Fatal(err)
// 	// }

// 	// pointers := make([]allocator.Pointable, 0, 1000)
// 	// var totalAllocated uint32 = 0
// 	// var totalFreed uint32 = 0
// 	// for i := 0; i < 1000; i++ {
// 	// 	size := uint32(rand.Int31n(4096))
// 	// 	totalAllocated += size
// 	// 	ptr, err := a.Alloc(size)
// 	// 	if err != nil {
// 	// 		logrus.Fatal(err)
// 	// 	}
// 	// 	pointers = append(pointers, ptr)
// 	// 	// fmt.Println("alloc", ptr)
		
// 	// 	if rand.Int31n(2) == 0 {
// 	// 		totalFreed += size
// 	// 		i := rand.Intn(len(pointers))
// 	// 		ptr := pointers[i]
// 	// 		pointers[i] = pointers[len(pointers)-1]
// 	// 		pointers = pointers[:len(pointers)-1]
// 	// 		if err := a.Free(ptr); err != nil {
// 	// 			logrus.Fatal(err)
// 	// 		}
// 	// 		// fmt.Println("free")
// 	// 	}
// 	// }
// 	// allocFreeDuration := time.Since(start)

// 	// if err := a.Print(); err != nil {
// 	// 	logrus.Fatal(err)
// 	// }
// 	// fmt.Println("allocFreeDuration", allocFreeDuration)
// 	// fmt.Println("totalAllocated", totalAllocated)
// 	// fmt.Println("totalFreed", totalFreed)
// }

type binaryMarshalerUnmarshaler struct {
	dirty bool
	Item  interface{} `json:"item"`
}

func (b *binaryMarshalerUnmarshaler) IsDirty() bool {
	return b.dirty
}

func (b *binaryMarshalerUnmarshaler) Dirty(v bool) {
	b.dirty = v
}

func (b *binaryMarshalerUnmarshaler) Size() uint32 {
	bytes, _ := b.MarshalBinary()
	return uint32(len(bytes))
}

func (b *binaryMarshalerUnmarshaler) MarshalBinary() ([]byte, error) {
	return json.Marshal(b)
}

func (b *binaryMarshalerUnmarshaler) UnmarshalBinary(d []byte) error {
	return json.Unmarshal(d, &b)
}

func sprintData(columns []*column.Column, data []map[string]types.DataType) string {
	str := ""
	for _, d := range data {
		for _, col := range columns {
			str += fmt.Sprintf("'%s' -> '%v', ", col.Name, d[col.Name].Value())
		}
		str += "\n"
	}
	return str
}

func printData(columns []*column.Column, data []map[string]types.DataType) {
	fmt.Print(sprintData(columns, data))
}

func randomString(length int) string {
	bytes := make([]byte, 0, length)
	for i := 0; i < length; i++ {
		bytes = append(bytes, byte('a' + rand.Intn(int('z') - int('a'))))
	}
	return string(bytes)
}
