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
	"encoding/binary"
	"os"

	"go-dbms/pkg/data"

	"github.com/sirupsen/logrus"
)

func main() {
	logrus.SetLevel(logrus.DebugLevel)
	fileName := "df.dat"
	// _ = os.Remove(fileName)

	logrus.Debugf("using file '%s'...\n", fileName)

	df, err := data.Open(fileName, &data.Options{
		ReadOnly: false,
		FileMode: 0664,
		PageSize: os.Getpagesize(),
		// PageSize: 64,
		PreAlloc: 100,
		Columns:  map[string]int{
			"id": data.TYPE_INT,
			"name": data.TYPE_STRING,
		},
	})
	if err != nil {
		logrus.Fatalf("failed to init B+ tree: %v", err)
	}
	defer func() {
		_ = df.Close()
		// _ = os.Remove(fileName)
	}()

	// idBytes := make([]byte, 4)
	// binary.BigEndian.PutUint32(idBytes, uint32(17))
	// id, err := df.Put([][]byte{
	// 	idBytes,
	// 	[]byte(strings.Repeat("J", 9000)),
	// })
	// if err != nil {
	// 	logrus.Fatal(err)
	// }
	// logrus.Debug("id => ", id)

	id := 88 // 91
	data, err := df.Get(id)
	if err != nil {
		logrus.Fatal(err)
	}
	logrus.Debugf(
		"[%v] got id => '%v', got name => '%v', name length => '%v'",
		id,
		binary.BigEndian.Uint32(data[0]),
		string(data[1][0]),
		len(data[1]),
	)



	// start := time.Now()

	// for i := 0; i < 1000; i++ {
	// 	idBytes := make([]byte, 4)
	// 	binary.BigEndian.PutUint32(idBytes, uint32(i))
	// 	id, err := df.Put([][]byte{
	// 		idBytes,
	// 		[]byte("second"),
	// 	})
	// 	if err != nil {
	// 		logrus.Fatal(err)
	// 	}
	// 	logrus.Debug("id => ", id)
	// }

	// err = df.WriteAll()
	// if err != nil {
	// 	logrus.Fatal(err)
	// }

	// logrus.Debug(time.Since(start))

	// for i := 2; i < 103; i++ {
	// 	data, err := df.Get(i)
	// 	if err != nil {
	// 		logrus.Fatal(err)
	// 	}
	// 	logrus.Debugf(
	// 		"[%v] got id => '%v', got name => '%v'",
	// 		i,
	// 		binary.BigEndian.Uint32(data[0]),
	// 		string(data[1]),
	// 	)
	// }
}
