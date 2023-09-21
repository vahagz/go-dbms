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

package main

import (
	"fmt"
	"os"
	"path"
	"time"

	"go-dbms/pkg/column"
	"go-dbms/pkg/table"
	"go-dbms/pkg/types"

	"github.com/sirupsen/logrus"
)

func main() {
	logrus.SetLevel(logrus.DebugLevel)

	dir, _ := os.Getwd()
	tablePath := path.Join(dir, "testtable")
	var options *table.Options = nil

	idMeta := types.Meta(types.TYPE_INT, false, true, 4)
	fnMeta := types.Meta(types.TYPE_STRING, false)
	lnMeta := types.Meta(types.TYPE_STRING, false)

	options = &table.Options{
		Columns: []*column.Column{
			{
				Name: "id",
				Typ:  types.TYPE_INT,
				Meta: idMeta,
			},
			{
				Name: "firstname",
				Typ:  types.TYPE_STRING,
				Meta: fnMeta,
			},
			{
				Name: "lastname",
				Typ:  types.TYPE_STRING,
				Meta: lnMeta,
			},
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
		// os.Remove(path.Join(tablePath, "data.dat"))
		// os.RemoveAll(path.Join(tablePath, "indexes"))
	}()


	// ptr, err := table.Insert(map[string]types.DataType{
	// 	"id":        types.Type(types.TYPE_INT).Set(int32(7)),
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
	// 	fmt.Printf("%s, %s", *ptr, sprintData(table.Columns(), [][]types.DataType{row}))
	// 	return false
	// })
	// if err != nil {
	// 	logrus.Fatal(err)
	// }



	// err = table.CreateIndex(nil, []string{"id"}, false, 8)
	// if err != nil {
	// 	logrus.Fatal(err)
	// }
	// err = table.CreateIndex(nil, []string{"firstname","lastname"}, false, 50)
	// if err != nil {
	// 	logrus.Fatal(err)
	// }



	// record, err := table.FindOneByIndex(map[string]types.DataType{
	// 	"id": types.Type(types.TYPE_INT).Set(int32(50)),
	// }, "id_1")
	// if err != nil {
	// 	logrus.Fatal(err)
	// }
	// printData(options.ColumnsOrder, [][]types.DataType{record})



	// rand.Seed(time.Now().Unix())
	// ids      := []int{5,6,4,5,7,2,1,9}
	// names    := []string{"Vahag",     "Sergey",    "Bagrat",   "Mery"}
	// surnames := []string{"Zargaryan", "Voskanyan", "Galstyan", "Sargsyan"}
	// for _, id := range ids {
	// 	ptr, err := table.Insert(map[string]types.DataType{
	// 		"id":        types.Type(types.TYPE_INT,    table.ColumnsMap()["id"].Meta).Set(id),
	// 		"firstname": types.Type(types.TYPE_STRING, table.ColumnsMap()["firstname"].Meta).Set(names[rand.Int31n(4)]),
	// 		"lastname":  types.Type(types.TYPE_STRING, table.ColumnsMap()["lastname"].Meta).Set(surnames[rand.Int31n(4)]),
	// 	})
	// 	if err != nil {
	// 		logrus.Error(err)
	// 	}
	// 	logrus.Debugf("%s", ptr)
	// }
	
	// err = table.FullScanByIndex("id_1", false, func(ptr *data.RecordPointer, row []types.DataType) (bool, error) {
	// 	fmt.Printf("%s, %s", *ptr, sprintData(table.Columns(), [][]types.DataType{row}))
	// 	return false, nil
	// })
	// if err != nil {
	// 	logrus.Fatal(err)
	// }

	records, err := table.FindByIndex("firstname_lastname_1", true, map[string]types.DataType{
		"firstname": types.Type(types.TYPE_STRING, table.ColumnsMap()["firstname"].Meta).Set("Sergey"),
		"lastname":  types.Type(types.TYPE_STRING, table.ColumnsMap()["lastname"].Meta).Set("Zargaryan"),
	})
	if err != nil {
		logrus.Fatal(err)
	}
	printData(table.Columns(), records)

	// for i := 0; i < 10; i++ {
	// 	record, err := table.FindByIndex("id_1", false, map[string]types.DataType{
	// 		"id": types.Type(types.TYPE_INT, table.ColumnsMap()["id"].Meta).Set(i),
	// 	})
	// 	if err != nil {
	// 		logrus.Error(err)
	// 		continue
	// 	}
	// 	printData(table.Columns(), record)
	// }
}


func sprintData(columns []*column.Column, data [][]types.DataType) string {
	str := ""
	for _, d := range data {
		for i, col := range columns {
			str += fmt.Sprintf("'%s' -> '%v', ", col.Name, d[i].Value())
		}
		str += "\n"
	}
	return str
}

func printData(columns []*column.Column, data [][]types.DataType) {
	fmt.Println(sprintData(columns, data))
}
