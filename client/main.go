package main

import (
	"encoding/binary"
	"fmt"
	r "math/rand"
	"net"
	"os"
	"time"

	"go-dbms/client/types"
	"go-dbms/client/util/response"

	"github.com/pkg/errors"
)

var rand = r.New(r.NewSource(time.Now().UnixNano()))

func New(host, port, user, pass string) (*Client, error) {
	tcpServer, err := net.ResolveTCPAddr("tcp", host+":"+port)

	if err != nil {
		return nil, errors.Wrap(err, "ResolveTCPAddr failed")
	}

	conn, err := net.DialTCP("tcp", nil, tcpServer)
	if err != nil {
		return nil, errors.Wrap(err, "Dial failed")
	}

	err = conn.SetKeepAlive(true)
	if err != nil {
		return nil, errors.Wrap(err, "Unable to set keepalive")
	}

	c := &Client{conn}
	return c, c.auth(user, pass)
}

type Client struct {
	conn *net.TCPConn
}

func (c *Client) Query(b []byte) (*types.Rows, error) {
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, uint32(len(b)))

	if _, err := c.conn.Write(header); err != nil {
		fmt.Println("Error while sending header: ", err)
		return nil, errors.New("Response error")
	}

	if _, err := c.conn.Write(b); err != nil {
		fmt.Println("Error while sending data: ", err)
		return nil, errors.New("Response error")
	}

	return &types.Rows{Conn: c.conn, Res: response.NewReader(c.conn)}, nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}

func (c *Client) auth(user, pass string) error {
	rows, err := c.Query([]byte(fmt.Sprintf("%s:%s", user, pass)))
	if err != nil {
		return errors.Wrap(err, "auth failed")
	}

	for rows.Next() {  }
	fmt.Println("[auth]")
	return nil
}

func main() {
	client, err := New("localhost", "8080", "username", "password")
	if err != nil {
		exitIfErr(err)
	}
	defer client.Close()

	var t time.Time
	var rows *types.Rows
	_, _ = rows, t

	// // t = time.Now()
	// // rows, err = client.Query([]byte(`PREPARE TABLE testtable ROWS 1000000;`))
	// // exitIfErr(errors.Wrap(err, "query failed"))
	// // for rows.Next() {  }
	// // fmt.Printf("[prepare] %v\n", time.Since(t))

	// t = time.Now()
	// rows, err = client.Query([]byte(`
	// 	CREATE TABLE testtable (
	// 		id        UInt32 AUTO INCREMENT,
	// 		firstname VARCHAR(32),
	// 		lastname  VARCHAR(32),
	// 		amount    Float64,
	// 	)
	// 	PRIMARY KEY(id) id,
	// 	INDEX(firstname, lastname) firstname_lastname;
	// `))
	// exitIfErr(errors.Wrap(err, "query failed"))
	// for rows.Next() {  }
	// fmt.Printf("[create] %v\n", time.Since(t))

	// t = time.Now()
	// var insertId int
	// firstnames := []string{"Vahag","Sergey","Bagrat","Mery"}
	// lastnames := []string{"Zargaryan","Galstyan","Sargsyan","Voskanyan"}
	// query := &bytes.Buffer{}
	// // setInterval(time.Second, func() {
	// // 	fmt.Println("[interval]", insertId)
	// // })
	// for i := 0; i < 10; i++ {
	// 	query.Reset()
	// 	query.WriteString("INSERT INTO testtable (firstname, lastname, amount) VALUES")
	// 	for i := 0; i < 1000; i++ {
	// 		query.WriteString(fmt.Sprintf(
	// 			"\n(%q,%q,%f),",
	// 			firstnames[rand.Intn(len(firstnames))],
	// 			lastnames[rand.Intn(len(lastnames))],
	// 			100 * rand.Float64(),
	// 		))
	// 	}
	// 	query.Truncate(query.Len() - 1)
	// 	query.WriteByte(';')
	// 	rows, err = client.Query(query.Bytes())
	// 	exitIfErr(errors.Wrap(err, "query failed"))
	// 	for rows.Next() {
	// 		rows.Scan(&insertId)
	// 		// fmt.Println(insertId)
	// 	}
	// }
	// fmt.Printf("[insert] %v\n", time.Since(t))

	t = time.Now()
	rows, err = client.Query([]byte(`
		SELECT firstname, COUNT(), SUM(amount), AVG(amount), MAX(amount), MIN(amount)
		// SELECT id, firstname, lastname
		FROM testtable
		WHERE_INDEX id id >= 1 AND id <= 1000
		// WHERE RES(id, 1) = 0 OR (firstname = "Vahag" AND lastname = "Zargaryan")
		;
	`))
	exitIfErr(errors.Wrap(err, "query failed"))
	var (
		id, cnt, avgId int
		avgAmount, sumAmount, maxAmount, minAmount float64
		firstname, lastname string
	)
	_, _, _, _, _, _, _ = id, cnt, sumAmount, firstname, lastname, avgId, avgAmount
	for rows.Next() {
		if err := rows.Scan(&firstname, &cnt, &sumAmount, &avgAmount, &maxAmount, &minAmount); err != nil {
			exitIfErr(errors.Wrap(err, "scan failed"))
		}
		fmt.Printf("%s %v %f %f %f %f\n", firstname, cnt, sumAmount, avgAmount, maxAmount, minAmount)
	}
	fmt.Printf("[select] %v\n", time.Since(t))

	// t = time.Now()
	// rows, err = client.Query([]byte(`
	// 	UPDATE testtable
	// 	SET firstname = "Bagrat"
	// 	WHERE_INDEX id id >= 1 AND id <= 1000
	// 	WHERE firstname = "Mery";
	// `))
	// exitIfErr(errors.Wrap(err, "query failed"))
	// for rows.Next() {  }
	// fmt.Printf("[update] %v\n", time.Since(t))

	// t = time.Now()
	// rows, err = client.Query([]byte(`
	// 	DELETE FROM testtable
	// 	WHERE_INDEX id id >= 1 AND id <= 1000
	// 	WHERE firstname = "Vahag";
	// `))
	// exitIfErr(errors.Wrap(err, "query failed"))
	// for rows.Next() {  }
	// fmt.Printf("[delete] %v\n", time.Since(t))
}

func exitIfErr(err error) {
	if err != nil {
		fmt.Println("error: ", err)
		os.Exit(1)
	}
}

func setInterval(duration time.Duration, f func()) *time.Ticker {
	t := time.NewTicker(duration)
	go func() {
		for range t.C {
			f()
		}
	}()
	return t
}

func setTimeout(duration time.Duration, f func()) *time.Ticker {
	var t *time.Ticker
	t = setInterval(duration, func() {
		f()
		t.Stop()
	})
	return t
}
