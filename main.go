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
// 	"os"

// 	"go-dbms/pkg/bptree"
// 	"go-dbms/pkg/types"

// 	"github.com/sirupsen/logrus"
// )

// func main() {
// logrus.SetLevel(logrus.DebugLevel)
// fileName := "bpt_index.idx"

// logrus.Debugf("using file '%s'...\n", fileName)

// tree, err := bptree.Open(fileName, &bptree.Options{
// 	ReadOnly:     false,
// 	FileMode:     0664,
// 	MaxKeySize:   8,
// 	MaxValueSize: 10,
// 	PageSize:     os.Getpagesize(),
// 	PreAlloc:     100,
// })
// if err != nil {
// 	logrus.Fatalf("failed to init B+ tree: %v", err)
// }
// defer func() {
// 	_ = tree.Close()
// 	// _ = os.Remove(fileName)
// }()

// rand.Seed(time.Now().Unix())
// for i := 0; i < 100; i++ {
// 	key := make([]byte, 8)
// 	binary.BigEndian.PutUint64(key, uint64(i))
// 	val := make([]byte, 10)
// 	binary.BigEndian.PutUint64(val[0:8], uint64(i))
// 	binary.BigEndian.PutUint16(val[8:10], uint16(rand.Intn(128)))
// 	err := tree.Put(key, val)
// 	if err != nil {
// 		logrus.Fatal(err)
// 	}
// }

// for i := 0; i < 100; i++ {
// 	key := make([]byte, 8)
// 	binary.BigEndian.PutUint64(key, uint64(i))
// 	val, err := tree.Get(key)
// 	if err != nil {
// 		logrus.Fatal(err)
// 	}
// 	logrus.Debug(binary.BigEndian.Uint64(val[0:8]), binary.BigEndian.Uint16(val[8:10]))
// }
// }

// package main

// import (
// 	"fmt"

// 	"go-dbms/pkg/types"
// )

// func main() {
// 	logrus.SetLevel(logrus.DebugLevel)
// 	fileName := "df.dat"
// 	// _ = os.Remove(fileName)

// 	logrus.Debugf("using file '%s'...\n", fileName)

// 	columnsOrder := []string{"id","name","surname"}
// 	_ = columnsOrder
// 	columns := map[string]types.TypeCode{
// 		"id":      types.TYPE_INT32,
// 		"name":    types.TYPE_STRING,
// 		"surname": types.TYPE_STRING,
// 	}

// 	df, err := data.Open(fileName, &data.Options{
// 		ReadOnly: false,
// 		FileMode: 0664,
// 		PageSize: os.Getpagesize(),
// 		PreAlloc: 10,
// 		Columns:  columns,
// 	})
// 	if err != nil {
// 		logrus.Fatalf("failed to init df: %v", err)
// 	}

// 	start := time.Now()
// 	defer func() {
// 		logrus.Debug(time.Since(start))
// 		logrus.Debug(df.FreeList())
// 		_ = df.Close()
// 	}()

// rand.Seed(time.Now().Unix())
// names    := []string{"Vahag",     "Sergey",    "Bagrat",   "Mery"}
// surnames := []string{"Zargaryan", "Voskanyan", "Galstyan", "Sargsyan"}
// for i := 0; i < 100; i++ {
// 	v1 := types.Type(types.TYPE_INT32);  v1.Set(int32(i))
// 	v2 := types.Type(types.TYPE_STRING); v2.Set(names[rand.Int31n(4)])
// 	v3 := types.Type(types.TYPE_STRING); v3.Set(surnames[rand.Int31n(4)])
// 	id, err := df.InsertRecord([]types.DataType{v1, v2, v3})
// 	if err != nil {
// 		logrus.Debug(df.FreeList())
// 		logrus.Fatal(err)
// 	}
// 	logrus.Debug("id => ", id)
// }

// id := 4
// data, err := df.GetPage(id)
// if err != nil {
// 	logrus.Fatal(err)
// }
// logrus.Debug(len(data))
// logrus.Debugf("[%v] %s", id, sprintData(columnsOrder, data))

// err = df.Scan(func(pageId, slotId int, row []types.DataType) bool {
// 	logrus.Debugf("[%v][%v] %s", pageId, slotId, sprintData(columnsOrder, [][]types.DataType{row}))
// 	return false
// })
// if err != nil {
// 	logrus.Fatal(err)
// }

// id := 4
// data, err := df.GetPage(id)
// if err != nil {
// 	logrus.Fatal(err)
// }
// logrus.Debug(len(data))
// last := data[len(data)-1]
// last[2].Set(last[2].Value().(string) + "dsadsads")
// if moved, err := df.UpdatePage(id, data); err != nil {
// 	logrus.Fatal(err)
// } else {
// 	for pid, v := range moved {
// 		printData(pid, columnsOrder, [][]types.DataType{v})
// 	}
// }

// if err := df.DeletePage(8); err != nil {
// 	logrus.Error(err)
// }

// }

package main

import (
	"fmt"
	"os"
	"path"
	"time"

	"go-dbms/pkg/table"
	"go-dbms/pkg/types"

	"github.com/sirupsen/logrus"
)

func main() {
	logrus.SetLevel(logrus.DebugLevel)

	dir, _ := os.Getwd()
	tablePath := path.Join(dir, "testtable")
	options := &table.Options{
		ColumnsOrder: []string{"id","firstname","lastname"},
		Columns: map[string]types.TypeCode{
			"id": types.TYPE_INT32,
			"firstname": types.TYPE_STRING,
			"lastname": types.TYPE_STRING,
		},
	}
	table, err := table.Open(tablePath, options)
	if err != nil {
		logrus.Fatal(err)
	}

	start := time.Now()
	defer func() {
		logrus.Debug(time.Since(start))
		_ = table.Close()
	}()


	// ptr, err := table.Insert(map[string]types.DataType{
	// 	"id":        types.Type(types.TYPE_INT32).Set(int32(7)),
	// 	"firstname": types.Type(types.TYPE_STRING).Set("Vahag"),
	// 	"lastname":  types.Type(types.TYPE_STRING).Set("Zargaryan"),
	// })
	// if err != nil {
	// 	logrus.Fatal(err)
	// }

	// fmt.Printf("%s\n", ptr)
	// record, err := table.Get(ptr)
	// if err != nil {
	// 	logrus.Fatal(err)
	// }
	// printData(options.ColumnsOrder, [][]types.DataType{record})



	// err = table.FullScan(func(ptr *data.RecordPointer, row []types.DataType) bool {
	// 	fmt.Printf("%s, %s", *ptr, sprintData(options.ColumnsOrder, [][]types.DataType{row}))
	// 	return false
	// })
	// if err != nil {
	// 	logrus.Fatal(err)
	// }



	// err = table.CreateIndex(nil, []string{"id"}, false)
	// if err != nil {
	// 	logrus.Fatal(err)
	// }



	// record, err := table.FindOneByIndex(map[string]types.DataType{
	// 	"id": types.Type(types.TYPE_INT32).Set(int32(50)),
	// }, "id_1")
	// if err != nil {
	// 	logrus.Fatal(err)
	// }
	// printData(options.ColumnsOrder, [][]types.DataType{record})



	// rand.Seed(time.Now().Unix())
	// names    := []string{"Vahag",     "Sergey",    "Bagrat",   "Mery"}
	// surnames := []string{"Zargaryan", "Voskanyan", "Galstyan", "Sargsyan"}
	// for i := 0; i < 100; i++ {
	// 	ptr, err := table.Insert(map[string]types.DataType{
	// 		"id":        types.Type(types.TYPE_INT32).Set(int32(i)),
	// 		"firstname": types.Type(types.TYPE_STRING).Set(names[rand.Int31n(4)]),
	// 		"lastname":  types.Type(types.TYPE_STRING).Set(surnames[rand.Int31n(4)]),
	// 	})
	// 	if err != nil {
	// 		logrus.Fatal(err)
	// 	}
	// 	logrus.Debugf("%s", ptr)
	// }

	for i := 0; i < 100; i++ {
		record, err := table.FindOneByIndex(map[string]types.DataType{
			"id": types.Type(types.TYPE_INT32).Set(int32(i)),
		}, "id_1")
		if err != nil {
			logrus.Fatal(err)
		}
		printData(options.ColumnsOrder, [][]types.DataType{record})
	}
}


func sprintData(columnsOrder []string, data [][]types.DataType) string {
	str := ""
	for _, d := range data {
		for i, col := range columnsOrder {
			str += fmt.Sprintf("'%s' -> '%v', ", col, d[i].Value())
		}
		str += "\n"
	}
	return str
}

func printData(columnsOrder []string, data [][]types.DataType) {
	fmt.Println(sprintData(columnsOrder, data))
}
