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
		ReadOnly:   false,
		FileMode:   0664,
		PageSize:   os.Getpagesize(),
		PreAlloc:   100,
	})
	if err != nil {
		logrus.Fatalf("failed to init B+ tree: %v", err)
	}
	defer func() {
		_ = df.Close()
		// _ = os.Remove(fileName)
	}()


	// logrus.Debug(df)

	// idBytes := make([]byte, 4)
	// binary.LittleEndian.PutUint32(idBytes, 2)
	// id, err := df.Put([][]byte{
	// 	idBytes,
	// 	[]byte("second"),
	// })
	// if err != nil {
	// 	logrus.Fatal(err)
	// }
	// logrus.Debug("id => ", id)

	data, err := df.Get(100)
	if err != nil {
		logrus.Fatal(err)
	}
	logrus.Debug("got id => ", binary.LittleEndian.Uint32(data[0]))
	logrus.Debug("got name => ", string(data[1]))
}
