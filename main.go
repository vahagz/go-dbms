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
// 	"os"

// 	"go-dbms/pkg/bptree"
// 	"go-dbms/util/helpers"

// 	"github.com/sirupsen/logrus"
// )

// func main() {
// 	logrus.SetLevel(logrus.DebugLevel)
// 	fileName := "kiwi_bptree.idx"
// 	_ = os.Remove(fileName)

// 	logrus.Debugf("using file '%s'...\n", fileName)

// 	tree, err := bptree.Open(fileName, &bptree.Options{
// 		ReadOnly:   false,
// 		FileMode:   0664,
// 		MaxKeySize: 4,
// 		PageSize:   os.Getpagesize(),
// 		PreAlloc:   100,
// 	})
// 	if err != nil {
// 		logrus.Fatalf("failed to init B+ tree: %v", err)
// 	}
// 	defer func() {
// 		_ = tree.Close()
// 		// _ = os.Remove(fileName)
// 	}()

// 	count := uint32(10000)
// 	writeTime, err := helpers.WriteALot(tree, count)
// 	if err != nil {
// 		logrus.Errorf("error while Put(): %v", err)
// 	}
// 	logrus.Debugf("took %s to Put %d entris", writeTime, count)

// 	scanTime, err := helpers.ScanALot(tree, count)
// 	if err != nil {
// 		logrus.Errorf("error while Scan(): %v", err)
// 	}
// 	logrus.Debugf("took %s to Scan %d entris", scanTime, count)

// 	readTime, err := helpers.ReadALot(tree, count)
// 	if err != nil {
// 		logrus.Errorf("error while Get(): %v", err)
// 	}
// 	logrus.Debugf("took %s to Get %d entris", readTime, count)
// }

package main

import (
	"fmt"
	"math/rand"
	"os"
	"time"

	data "go-dbms/pkg/slotted_data"
	"go-dbms/pkg/types"

	"github.com/sirupsen/logrus"
)

func main() {
	logrus.SetLevel(logrus.DebugLevel)
	fileName := "df.dat"
	// _ = os.Remove(fileName)

	logrus.Debugf("using file '%s'...\n", fileName)

	columnsOrder := []string{"id","name","surname"}
	_ = columnsOrder
	columns := map[string]types.TypeCode{
		"id":      types.TYPE_INT32,
		"name":    types.TYPE_STRING,
		"surname": types.TYPE_STRING,
	}

	df, err := data.Open(fileName, &data.Options{
		ReadOnly: false,
		FileMode: 0664,
		PageSize: os.Getpagesize(),
		// PageSize: 64,
		PreAlloc: 10,
		Columns:  columns,
	})
	if err != nil {
		logrus.Fatalf("failed to init df: %v", err)
	}

	start := time.Now()
	defer func() {
		logrus.Debug(time.Since(start))
		logrus.Debug(df.FreeList())
		_ = df.Close()
	}()


	rand.Seed(time.Now().Unix())
	names    := []string{"Vahag",     "Sergey",    "Bagrat",   "Mery"}
	surnames := []string{"Zargaryan", "Voskanyan", "Galstyan", "Sargsyan"}
	for i := 0; i < 100; i++ {
		v1 := types.Type(types.TYPE_INT32);  v1.Set(int32(i))
		v2 := types.Type(types.TYPE_STRING); v2.Set(names[rand.Int31n(4)])
		v3 := types.Type(types.TYPE_STRING); v3.Set(surnames[rand.Int31n(4)])
		id, err := df.InsertRecord([]types.DataType{v1, v2, v3})
		if err != nil {
			logrus.Debug(df.FreeList())
			logrus.Fatal(err)
		}
		logrus.Debug("id => ", id)
	}


	// id := 4
	// data, err := df.GetPage(id)
	// if err != nil {
	// 	logrus.Fatal(err)
	// }
	// logrus.Debug(len(data))
	// printData(id, columnsOrder, data)


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

}


func printData(pid int, columnsOrder []string, data [][]types.DataType) {
	for _, d := range data {
		str := fmt.Sprintf("[%v]", pid)
		for i, col := range columnsOrder {
			str += fmt.Sprintf(" '%s' -> '%v', ", col, d[i].Value())
		}
		logrus.Debug(str)
	}
}
