package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"go-dbms/pkg/bptree"
	"go-dbms/pkg/column"
	"go-dbms/pkg/types"
	r "math/rand"
	"os"
	"path"
	"time"

	"github.com/sirupsen/logrus"
)

var rand = r.New(r.NewSource(time.Now().Unix()))

// func main() {
// 	configs := config.New()
// 	svcs := services.New()
// 	err := server.Start(configs.ServerConfig, svcs)
// 	fmt.Printf(err)
// }




// func main() {
// 	logrus.SetLevel(logrus.DebugLevel)

// 	dir, _ := os.Getwd()
// 	tablePath := path.Join(dir, "testtable")
// 	var options *table.Options = nil

// 	options = &table.Options{
// 		Columns: []*column.Column{
// 			column.New("id",        types.Meta(types.TYPE_INTEGER, true, 4)),
// 			column.New("firstname", types.Meta(types.TYPE_VARCHAR, 32)),
// 			column.New("lastname",  types.Meta(types.TYPE_VARCHAR, 32)),
// 		},
// 	}

// 	table, err := table.Open(tablePath, options)
// 	if err != nil {
// 		logrus.Fatal(err)
// 	}

// 	start := time.Now()
// 	exitFunc := func() {
// 		fmt.Println("DURATION =>", time.Since(start))
// 		_ = table.Close()
// 		// os.Remove(path.Join(tablePath, "data.dat"))
// 		// os.RemoveAll(path.Join(tablePath, "indexes"))
// 	}
// 	logrus.RegisterExitHandler(exitFunc)
// 	defer exitFunc()

// 	// ptr, err := table.Insert(map[string]types.DataType{
// 	// 	"id":        types.Type(types.TYPE_INT).Set(int32(7)),
// 	// 	"firstname": types.Type(types.TYPE_STRING).Set("Vahag"),
// 	// 	"lastname":  types.Type(types.TYPE_STRING).Set("Zargaryan"),
// 	// })
// 	// if err != nil {
// 	// 	logrus.Fatal(err)
// 	// }

// 	// fmt.Printf("%s\n", ptr)
// 	// record, err := table.Get(ptr)
// 	// if err != nil {
// 	// 	logrus.Fatal(err)
// 	// }
// 	// printData(options.ColumnsOrder, [][]types.DataType{record})

// 	// err = table.FullScan(func(ptr *data.RecordPointer, row map[string]types.DataType) (bool, error) {
// 	// 	fmt.Printf("%s, %s", ptr, sprintData(table.Columns(), []map[string]types.DataType{row}))
// 	// 	return false, nil
// 	// })
// 	// if err != nil {
// 	// 	logrus.Fatal(err)
// 	// }

// 	err = table.CreateIndex(nil, []string{"id"}, false)
// 	if err != nil {
// 		logrus.Fatal(err)
// 	}
// 	err = table.CreateIndex(nil, []string{"firstname","lastname"}, false)
// 	if err != nil {
// 		logrus.Fatal(err)
// 	}

// 	rand.Seed(time.Now().Unix())
// 	ids      := []int{5,6,4,5,7,2,1,9}
// 	names    := []string{"Vahag",     "Sergey",    "Bagrat",   "Mery"}
// 	surnames := []string{"Zargaryan", "Voskanyan", "Galstyan", "Sargsyan"}
// 	for _, id := range ids {
// 		_, err := table.Insert(map[string]types.DataType{
// 			"id":        types.Type(table.ColumnsMap()["id"].Meta).Set(id),
// 			"firstname": types.Type(table.ColumnsMap()["firstname"].Meta).Set(names[rand.Int31n(4)]),
// 			"lastname":  types.Type(table.ColumnsMap()["lastname"].Meta).Set(surnames[rand.Int31n(4)]),
// 		})
// 		if err != nil {
// 			logrus.Error(err)
// 		}
// 	}

// 	// err = table.FullScanByIndex("id_1", false, func(row map[string]types.DataType) (bool, error) {
// 	// 	printData(table.Columns(), []map[string]types.DataType{row})
// 	// 	return false, nil
// 	// })
// 	// if err != nil {
// 	// 	logrus.Fatal(err)
// 	// }

// 	// TODO: handle case when count of duplicate entries in node doesn't fit in page
// 	// TODO: add freelist logic
// 	err = table.FullScanByIndex("firstname_lastname_1", false, func(row map[string]types.DataType) (bool, error) {
// 		printData(table.Columns(), []map[string]types.DataType{row})
// 		return false, nil
// 	})
// 	if err != nil {
// 		logrus.Fatal(err)
// 	}

// 	// records, err := table.FindByIndex(
// 	// 	// "id_1",
// 	// 	"firstname_lastname_1",
// 	// 	"<=",
// 	// 	map[string]types.DataType{
// 	// 		// "id": types.Type(table.ColumnsMap()["id"].Meta).Set(5),
// 	// 		"firstname": types.Type(table.ColumnsMap()["firstname"].Meta).Set("Sergey"),
// 	// 		"lastname":  types.Type(table.ColumnsMap()["lastname"].Meta).Set("Zargaryan"),
// 	// 	},
// 	// )
// 	// if err != nil {
// 	// 	logrus.Fatal(err)
// 	// }
// 	// printData(table.Columns(), records)

// 	// for i := 0; i < 10; i++ {
// 	// 	record, err := table.FindByIndex("id_1", false, map[string]types.DataType{
// 	// 		"id": types.Type(types.TYPE_INT, table.ColumnsMap()["id"].Meta).Set(i),
// 	// 	})
// 	// 	if err != nil {
// 	// 		logrus.Error(err)
// 	// 		continue
// 	// 	}
// 	// 	printData(table.Columns(), record)
// 	// }
// }

func main() {
	logrus.SetLevel(logrus.DebugLevel)
	pwd, _ := os.Getwd()

	bptreeFile := path.Join(pwd, "test", "bptree")

	tree, err := bptree.Open(bptreeFile, &bptree.Options{
		PageSize: os.Getpagesize(),
		MaxKeySize: 4,
		MaxValueSize: 1,
		Degree: 250,
		KeyCols: 1,
	})
	if err != nil {
		logrus.Fatal(err)
	}

	var getDuration time.Duration
	var insertDuration time.Duration
	start := time.Now()
	exitFunc := func() {
		fmt.Println("\nINSERT DURATION =>", insertDuration)
		fmt.Println("\nGET DURATION =>", getDuration)
		fmt.Println("\nTOTAL DURATION =>", time.Since(start))
		if err := tree.Close(); err != nil {
			logrus.Error(err)
		}
	}
	logrus.RegisterExitHandler(exitFunc)
	defer exitFunc()

	list := make([][][]byte, 0, 1000)
	// list[0] = [][]byte{{60,112,5,128}}
	// list[1] = [][]byte{{60,186,116,180}}
	// list[2] = [][]byte{{1,103,110,244}}
	// list[3] = [][]byte{{60,89,35,18}}
	// list[4] = [][]byte{{126,57,101,226}}
	// list[5] = [][]byte{{26,198,9,116}}
	// list[6] = [][]byte{{42,105,119,168}}
	// list[7] = [][]byte{{71,107,143,109}}
	// list[8] = [][]byte{{95,199,70,86}}
	// list[9] = [][]byte{{113,161,116,94}}
	// list[10] = [][]byte{{39,143,167,182}}
	// tree.PrepareSpace(2*1024)
	for i := 0; i < 1000; i++ {
		key := make([]byte, 4)
		binary.BigEndian.PutUint32(key, uint32(rand.Int31()))
		list = append(list, [][]byte{key})
		err = tree.PutMem(list[i], []byte{list[i][0][1]}, &bptree.PutOptions{
			Uniq:   false,
			Update: false,
		})
		if err != nil {
			logrus.Fatal(err)
		}

		// go func(i int) {
			if val, err := tree.Get(list[i]); err != nil {
				fmt.Println(i, list[i], val, err)
			}
		// }(i)
	}
	if err := tree.WriteAll(); err != nil {
		logrus.Fatal(err)
	}
	insertDuration = time.Since(start)

	// keys := [][]byte{
	// 	{2,3,4,5},
	// 	{1,2,3,4},
	// 	{3,4,5,6},
	// 	{1,2,3,4},
	// 	{4,5,6,7},
	// 	{1,2,3,4},
	// 	{5,6,7,8},
	// 	{1,2,3,4},
	// 	{6,7,8,9},
	// 	{1,2,3,4},
	// 	{7,8,9,10},
	// 	{1,2,3,4},
	// 	{8,9,10,11},
	// 	{1,2,3,4},
	// 	{9,10,11,12},
	// 	{1,2,3,4},
	// 	{10,11,12,13},
	// 	{1,2,3,4},
	// 	{11,12,13,14},
	// 	{1,2,3,4},
	// 	{12,13,14,15},
	// }
	// _ = keys
	// for i := range keys {
	// 	err = tree.Put([][]byte{keys[i]}, []byte{byte(i)}, &bptree.PutOptions{
	// 		Uniq:   false,
	// 		Update: false,
	// 	})
	// 	if err != nil {
	// 		logrus.Fatal(err)
	// 	}
	// }
	
	// err = tree.Scan(nil, false, true, func(key [][]byte, val []byte) (bool, error) {
	// 	fmt.Println(key, val)
	// 	return false, nil
	// })
	// if err != nil {
	// 	logrus.Fatal(err)
	// }
	tree.Print()
	fmt.Println(tree.Count())


	// vals, err := tree.Del([][]byte{{6,7,8}})
	// if err != nil {
	// 	logrus.Fatal(err)
	// }
	// fmt.Println(vals)

	// err = tree.Scan(nil, false, true, func(key [][]byte, val []byte) (bool, error) {
	// 	fmt.Printf("key -> %v, val -> %v\n", key, val)
	// 	return false, nil
	// })
	// if err != nil {
	// 	logrus.Fatal(err)
	// }
	// fmt.Println(tree.Count())

	// if counter != 1000 {
	// 	bin, _ := json.Marshal(list)
	// 	fmt.Println(string(bin))
	// }
	// fmt.Println(counter)




	// pagerFile := path.Join(pwd, "test", "heap.dat")
	// p, err := pager.Open(pagerFile, os.Getpagesize(), false, 0644)
	// if err != nil {
	// 	logrus.Fatal(err)
	// }

	// allocatorFile := path.Join(pwd, "test", "freelist")
	// // os.Remove(allocatorFile)
	// a, err := allocator.Open(
	// 	allocatorFile,
	// 	&allocator.Options{
	// 		TargetPageSize: uint16(os.Getpagesize()),
	// 		TreePageSize:   uint16(os.Getpagesize()),
	// 		Pager:          p,
	// 	},
	// )
	// if err != nil {
	// 	logrus.Fatal(err)
	// }

	// c := cache.NewCache[binaryMarshalerUnmarshaler](3)

	// start := time.Now()
	// exitFunc := func() {
	// 	fmt.Println("\nTOTAL DURATION =>", time.Since(start))
	// 	if err := a.Close(); err != nil {
	// 		logrus.Error(err)
	// 	}
	// }
	// logrus.RegisterExitHandler(exitFunc)
	// defer exitFunc()


	// val := &binaryMarshalerUnmarshaler{Item: []int{rand.Intn(10),rand.Intn(10),rand.Intn(10)}}
	// ptr, err := a.Alloc(val.Size())
	// if err != nil {
	// 	logrus.Fatal(err)
	// }
	// cPtr := c.Add(ptr)
	// cPtr.Lock().Set(val).Flush()

	// for i := 0; i < 5; i++ {
	// 	val := &binaryMarshalerUnmarshaler{Item: []int{rand.Intn(10),rand.Intn(10),rand.Intn(10)}}
	// 	ptr, err := a.Alloc(val.Size())
	// 	if err != nil {
	// 		logrus.Fatal(err)
	// 	}
	// 	cPtr := c.Add(ptr)
	// 	cPtr.Lock().Set(val).Flush().Unlock()

	// 	fmt.Println("=====================================")
	// 	fmt.Println(c.Get(a.Pointer(23, 16)))
	// 	fmt.Println(c.Get(a.Pointer(49, 16)))
	// 	fmt.Println(c.Get(a.Pointer(75, 16)))
	// 	fmt.Println(c.Get(a.Pointer(101, 16)))
	// 	fmt.Println(c.Get(a.Pointer(127, 16)))
	// 	fmt.Println(c.Get(a.Pointer(153, 16)))
	// }

	// cPtr.Unlock()
	// fmt.Println("=====================================")
	// fmt.Println(c.Get(a.Pointer(23, 16)))
	// fmt.Println(c.Get(a.Pointer(49, 16)))
	// fmt.Println(c.Get(a.Pointer(75, 16)))
	// fmt.Println(c.Get(a.Pointer(101, 16)))
	// fmt.Println(c.Get(a.Pointer(127, 16)))
	// fmt.Println(c.Get(a.Pointer(153, 16)))

	// _ = c
	// val := &binaryMarshalerUnmarshaler{}
	// err = a.Pointer(153, 16).Get(val)
	// fmt.Println(val, err)


	// cPtr := c.Add(a.Pointer(49, 16))
	// cPtr.RLock()
	// go func() {
	// 	time.Sleep(5*time.Second)
	// 	fmt.Println("RUnlock", cPtr.Get())
	// 	cPtr.RUnlock()
	// }()

	// cPtr.Lock()
	// fmt.Println("Lock", cPtr.Get())
	// cPtr.Unlock()



	// ptr1, err := a.Alloc(4050)
	// if err != nil {
	// 	logrus.Fatal(err)
	// }
	// fmt.Println("alloc 1", ptr1)

	// ptr2, err := a.Alloc(100)
	// if err != nil {
	// 	logrus.Fatal(err)
	// }
	// fmt.Println("alloc 2", ptr2)

	// if err := a.Free(ptr2); err != nil {
	// 	logrus.Fatal(err)
	// }
	// fmt.Println("free 2")
	
	// ptr3, err := a.Alloc(2000)
	// if err != nil {
	// 	logrus.Fatal(err)
	// }
	// fmt.Println("alloc 3", ptr3)


	// item1 := &binaryMarshalerUnmarshaler{
	// 	map[string][]int{"dsadsads":{1,2,3}},
	// }
	// pt, err := a.Alloc(uint32(item1.Size()))
	// if err != nil {
	// 	logrus.Fatal(err)
	// }
	// ptr := allocator.Wrap[binaryMarshalerUnmarshaler](pt)
	// fmt.Println(ptr)
	// ptr.Set(item1)
	// fmt.Println(ptr.Get())


	// ptr := allocator.Wrap[binaryMarshalerUnmarshaler](a.Pointer(77, uint32(item1.Size())))
	// fmt.Println(ptr)
	// fmt.Println(ptr.Get())


	// fmt.Println(ptr)
	// if err := a.Free(ptr); err != nil {
	// 	logrus.Fatal(err)
	// }


	// if ptr, err := a.Alloc(1024 * 1024); err != nil {
	// 	logrus.Fatal(err)
	// } else if err := a.Free(ptr); err != nil {
	// 	logrus.Fatal(err)
	// }

	// pointers := make([]allocator.Pointable, 0, 1000)
	// var totalAllocated uint32 = 0
	// var totalFreed uint32 = 0
	// for i := 0; i < 1000; i++ {
	// 	size := uint32(rand.Int31n(4096))
	// 	totalAllocated += size
	// 	ptr, err := a.Alloc(size)
	// 	if err != nil {
	// 		logrus.Fatal(err)
	// 	}
	// 	pointers = append(pointers, ptr)
	// 	// fmt.Println("alloc", ptr)
		
	// 	if rand.Int31n(2) == 0 {
	// 		totalFreed += size
	// 		i := rand.Intn(len(pointers))
	// 		ptr := pointers[i]
	// 		pointers[i] = pointers[len(pointers)-1]
	// 		pointers = pointers[:len(pointers)-1]
	// 		if err := a.Free(ptr); err != nil {
	// 			logrus.Fatal(err)
	// 		}
	// 		// fmt.Println("free")
	// 	}
	// }
	// allocFreeDuration := time.Since(start)

	// if err := a.Print(); err != nil {
	// 	logrus.Fatal(err)
	// }
	// fmt.Println("allocFreeDuration", allocFreeDuration)
	// fmt.Println("totalAllocated", totalAllocated)
	// fmt.Println("totalFreed", totalFreed)
}

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
