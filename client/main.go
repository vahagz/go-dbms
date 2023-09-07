package main

import (
	"bufio"
	"net"
	"os"
	"sync"

	logger "github.com/sirupsen/logrus"
	prefixed "github.com/x-cray/logrus-prefixed-formatter"
)

const (
	HOST = "localhost"
	PORT = "8080"
	TYPE = "tcp"
)

var logrus = &logger.Logger{
	Out:   os.Stderr,
	Level: logger.DebugLevel,
	Formatter: &prefixed.TextFormatter{
		TimestampFormat: "2006-01-02 15:04:05",
		FullTimestamp: true,
		ForceFormatting: true,
	},
}

func main() {
	tcpServer, err := net.ResolveTCPAddr(TYPE, HOST+":"+PORT)

	if err != nil {
		logrus.Error("ResolveTCPAddr failed:", err.Error())
		os.Exit(1)
	}

	conn, err := net.DialTCP(TYPE, nil, tcpServer)
	if err != nil {
		logrus.Error("Dial failed:", err.Error())
		os.Exit(1)
	}

	err = conn.SetKeepAlive(true)
	if err != nil {
		logrus.Error("Unable to set keepalive - ", err)	
		os.Exit(1)
	}

	_, err = conn.Write([]byte("username:password;"))
	if err != nil {
		logrus.Error("Write data failed:", err.Error())
		os.Exit(1)
	}

	received, err := bufio.NewReader(conn).ReadString('\x00')
	if err != nil {
		logrus.Error("Auth send failed ", err.Error())
		os.Exit(1)
	}
	if received != "auth_success\x00" {
		logrus.Error("Auth failed: ", received)
		os.Exit(1)
	}
	logrus.Info("Successfuly authed!")

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		for {
			received, err := bufio.NewReader(conn).ReadString('\x00')
			if err != nil {
				logrus.Error("Read data failed: ", err.Error())
				os.Exit(1)
			}

			logrus.Info("Received message: ", received)
		}
	}()

	_, err = conn.Write([]byte("'Thi\\'s i;'s a message;"))
	if err != nil {
		logrus.Error("Write data failed:", err.Error())
		os.Exit(1)
	}

	// for {
	// 	time.Sleep(1 * time.Second)

	// 	_, err = conn.Write([]byte("This is a message;"))
	// 	if err != nil {
	// 		logrus.Error("Write data failed:", err.Error())
	// 		os.Exit(1)
	// 	}
	// }

	wg.Wait()
	conn.Close()
}
