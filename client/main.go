package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"sync"
)

const (
	HOST = "localhost"
	PORT = "8080"
	TYPE = "tcp"
)

func main() {
	tcpServer, err := net.ResolveTCPAddr(TYPE, HOST+":"+PORT)

	if err != nil {
		fmt.Println("ResolveTCPAddr failed:", err.Error())
		os.Exit(1)
	}

	conn, err := net.DialTCP(TYPE, nil, tcpServer)
	if err != nil {
		fmt.Println("Dial failed:", err.Error())
		os.Exit(1)
	}

	err = conn.SetKeepAlive(true)
	if err != nil {
		fmt.Println("Unable to set keepalive - ", err)	
		os.Exit(1)
	}

	_, err = conn.Write([]byte("username:password;"))
	if err != nil {
		fmt.Println("Write data failed:", err.Error())
		os.Exit(1)
	}

	received, err := bufio.NewReader(conn).ReadString('\x00')
	if err != nil {
		fmt.Println("Auth send failed ", err.Error())
		os.Exit(1)
	}
	if received != "auth_success\x00" {
		fmt.Println("Auth failed: ", received)
		os.Exit(1)
	}
	fmt.Println("Successfuly authed!")

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		for {
			received, err := bufio.NewReader(conn).ReadString('\x00')
			if err != nil {
				fmt.Println("Read data failed: ", err.Error())
				os.Exit(1)
			}

			fmt.Println("Received message: ", received)
		}
	}()

	_, err = conn.Write([]byte("'Thi\\'s i;'s a message;"))
	if err != nil {
		fmt.Println("Write data failed:", err.Error())
		os.Exit(1)
	}

	// for {
	// 	time.Sleep(1 * time.Second)

	// 	_, err = conn.Write([]byte("This is a message;"))
	// 	if err != nil {
	// 		fmt.Println("Write data failed:", err.Error())
	// 		os.Exit(1)
	// 	}
	// }

	wg.Wait()
	conn.Close()
}
