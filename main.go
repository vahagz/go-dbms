package main

import (
	"fmt"
	"go-dbms/pkg/column"
	"go-dbms/pkg/rbtree"
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

	file := path.Join(pwd, "test", "rbtree.bin")
	// os.Remove(file)
	t, err := rbtree.Open[*Pointer, *rbtree.DummyVal](
		file,
		&rbtree.Options{
			PageSize: uint16(os.Getpagesize()),
			KeySize:  10,
			ValSize:  0,
		},
	)
	if err != nil {
		logrus.Fatal(err)
	}

	entries := make([]*rbtree.Entry[*Pointer, *rbtree.DummyVal], 0)
	_ = entries
	start := time.Now()
	exitFunc := func() {
		fmt.Println("\nTOTAL DURATION =>", time.Since(start))
		_ = t.Close()
	}
	logrus.RegisterExitHandler(exitFunc)
	defer exitFunc()

	for i := 0; i < 10; i++ {
		entry := newEntry(uint16(rand.Int31n(256)), uint64(i))
		entries = append(entries, entry)
		if err := t.InsertMem(entry); err != nil {
			logrus.Fatal(err)
		}
	}
	if err := t.WriteAll(); err != nil {
		logrus.Fatal(err)
	}

	// // entries = []*rbtree.Entry[*Pointer, *rbtree.DummyVal]{}
	// for i := 0; i < len(entries); i++ {
	// 	if err := t.DeleteMem(entries[i].Key); err != nil {
	// 		logrus.Fatal(err)
	// 	}
	// }
	// if err := t.WriteAll(); err != nil {
	// 	logrus.Fatal(err)
	// }

	// keys := make([]*Pointer, 0, t.Count())
	// err = t.Scan(nil, func(entry *rbtree.Entry[*Pointer, *rbtree.DummyVal]) (bool, error) {
	// 	keys = append(keys, entry.Key)
	// 	return false, nil
	// })
	// for _, key := range keys {
	// 	if err := t.DeleteMem(key); err != nil {
	// 		logrus.Fatal(err)
	// 	}
	// }
	// if err := t.WriteAll(); err != nil {
	// 	logrus.Fatal(err)
	// }

	// db := make([]byte, 19)
	// binary.BigEndian.PutUint16(db, 191)
	// if err := t.Delete(db); err != nil {
	// 	logrus.Fatal(err)
	// }
	// binary.BigEndian.PutUint16(db, 227)
	// if err := t.Delete(db); err != nil {
	// 	logrus.Fatal(err)
	// }

	// gb := make([]byte, 19)
	// binary.BigEndian.PutUint16(gb, 242)
	// if v, err := t.Get(gb); err != nil {
	// 	logrus.Error(err)
	// } else {
	// 	fmt.Println(binary.BigEndian.Uint16(v))
	// }

	if err := t.Print(5); err != nil {
		logrus.Fatal(err)
	}
	err = t.Scan(nil, func(key *Pointer, val *rbtree.DummyVal) (bool, error) {
		fmt.Printf("(%d %d), ", key.freeSpace, key.pageId)
		return false, nil
	})
	if err != nil {
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
