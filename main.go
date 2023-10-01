// package main

// import (
// 	"fmt"
// 	"go-dbms/config"
// 	"go-dbms/server"
// 	"go-dbms/services"
// )

// func main() {
// 	configs := config.New()
// 	svcs := services.New()

// 	err := server.Start(configs.ServerConfig, svcs)
// 	fmt.Printf(err)
// }

// package main

// import (
// 	"fmt"
// 	r "math/rand"
// 	"os"
// 	"path"
// 	"time"

// 	"go-dbms/pkg/column"
// 	"go-dbms/pkg/table"
// 	"go-dbms/pkg/types"

// 	"github.com/sirupsen/logrus"
// )

// var rand = r.New(r.NewSource(time.Now().Unix()))

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

package main

import (
	"encoding/binary"
	"fmt"
	"go-dbms/pkg/column"
	"go-dbms/pkg/freelist"
	"go-dbms/pkg/types"
	r "math/rand"
	"os"
	"path"
	"time"

	"github.com/sirupsen/logrus"
)

var rand = r.New(r.NewSource(time.Now().Unix()))

func main() {
	logrus.SetLevel(logrus.DebugLevel)
	pwd, _ := os.Getwd()

	ll, err := freelist.Open(path.Join(pwd, "test", "freelist.bin"), &freelist.LinkedListOptions{
		PageSize: uint16(os.Getpagesize()),
		PreAlloc: 5,
		ValSize:  8,
	})
	if err != nil {
		logrus.Fatal(err)
	}

	// p, err := pager.Open(path.Join(pwd, "test", "test.dat"), os.Getpagesize(), false, 0664)
	// if err != nil {
	// 	logrus.Fatal(err)
	// }

	// subfl, err := freelist.Open(path.Join(pwd, "test", "freelist.bin"), &freelist.Options{
	// 	PreAlloc:         5,
	// 	TargetPageSize:   uint16(os.Getpagesize()),
	// 	FreelistPageSize: uint16(os.Getpagesize()),
	// })
	// if err != nil {
	// 	logrus.Fatal(err)
	// }

	// var fl freelist.Freelist
	// tree, err := bptree.Open(path.Join(pwd, "test", "bptree_freelist.idx"), &bptree.Options{
	// 	ReadOnly:     false,
	// 	FileMode:     0664,
	// 	MaxKeySize:   10,
	// 	MaxValueSize: 0,
	// 	PageSize:     os.Getpagesize(),
	// 	PreAlloc:     10,
	// 	FreelistOptions: &freelist.Options{
	// 		Allocator:      p,
	// 		PreAlloc:       5,
	// 		TargetPageSize: uint16(os.Getpagesize()),
	// 	},
	// }, subfl)
	// if err != nil {
	// 	logrus.Fatal(err)
	// }

	// fl = tree

	start := time.Now()
	exitFunc := func() {
		fmt.Println("TOTAL DURATION =>", time.Since(start))
		_ = ll.Close()
		// _ = p.Close()
	}
	logrus.RegisterExitHandler(exitFunc)
	defer exitFunc()


	// for i := 1; i <= 10; i++ {
	// 	// p.Alloc(i)
	// 	_, err = fl.AddMem(uint64(i), uint16(rand.Intn(4096)))
	// 	if err != nil {
	// 		logrus.Fatal(err)
	// 	}
	// }
	// fmt.Println("ADD DURATION =>", time.Since(start))
	// start = time.Now()
	// if err := fl.WriteAll(); err != nil {
	// 	logrus.Fatal(err)
	// }
	// fmt.Println("FLUSH DURATION =>", time.Since(start))

	// fmt.Println(fl.Get(&freelist.Pointer{
	// 	PageId: 4,
	// 	Index:  1,
	// }))

	// pageId, ptr, err := fl.Alloc(30)
	// fmt.Println(pageId, ptr, err)

	// if err = fl.Set(&bptree.Pointer{
	// 	FreeSpace: 1501,
	// 	PageId:    7,
	// }, 1000); err != nil {
	// 	logrus.Fatal(err)
	// }

	for i := 0; i < 10; i++ {
		val := make([]byte, 8)
		binary.BigEndian.PutUint64(val, uint64(rand.Int63n(100)))
		_, err := ll.Push(val)
		if err != nil {
			logrus.Fatal(err)
		}
	}
	
	if err = ll.Print(); err != nil {
		logrus.Fatal(err)
	}

	_, val, err := ll.Pop(2)
	if err != nil {
		logrus.Fatal(err)
	}
	fmt.Println(val)

	if err = ll.Print(); err != nil {
		logrus.Fatal(err)
	}
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
