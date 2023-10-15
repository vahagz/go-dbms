package main

import (
	"fmt"
	allocator "go-dbms/pkg/allocator/heap"
	"go-dbms/pkg/column"
	"go-dbms/pkg/pager"
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

	pagerFile := path.Join(pwd, "test", "heap.dat")
	p, err := pager.Open(pagerFile, os.Getpagesize(), false, 0644)
	if err != nil {
		logrus.Fatal(err)
	}

	allocatorFile := path.Join(pwd, "test", "freelist")
	// os.Remove(allocatorFile)
	a, err := allocator.Open(
		allocatorFile,
		&allocator.Options{
			TargetPageSize: uint16(os.Getpagesize()),
			TreePageSize:   uint16(os.Getpagesize()),
			Pager:          p,
		},
	)
	if err != nil {
		logrus.Fatal(err)
	}

	start := time.Now()
	exitFunc := func() {
		fmt.Println("\nTOTAL DURATION =>", time.Since(start))
		if err := a.Close(); err != nil {
			logrus.Error(err)
		}
	}
	logrus.RegisterExitHandler(exitFunc)
	defer exitFunc()

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

	if ptr, err := a.Alloc(1024 * 1024); err != nil {
		logrus.Fatal(err)
	} else if err := a.Free(ptr); err != nil {
		logrus.Fatal(err)
	}

	pointers := make([]allocator.Pointable, 0, 1000)
	var totalAllocated uint32 = 0
	var totalFreed uint32 = 0
	for i := 0; i < 1000; i++ {
		size := uint32(rand.Int31n(4096))
		totalAllocated += size
		ptr, err := a.Alloc(size)
		if err != nil {
			logrus.Fatal(err)
		}
		pointers = append(pointers, ptr)
		// fmt.Println("alloc", ptr)
		
		if rand.Int31n(2) == 0 {
			totalFreed += size
			i := rand.Intn(len(pointers))
			ptr := pointers[i]
			pointers[i] = pointers[len(pointers)-1]
			pointers = pointers[:len(pointers)-1]
			if err := a.Free(ptr); err != nil {
				logrus.Fatal(err)
			}
			// fmt.Println("free")
		}
	}
	allocFreeDuration := time.Since(start)

	if err := a.Print(); err != nil {
		logrus.Fatal(err)
	}
	fmt.Println("allocFreeDuration", allocFreeDuration)
	fmt.Println("totalAllocated", totalAllocated)
	fmt.Println("totalFreed", totalFreed)
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
