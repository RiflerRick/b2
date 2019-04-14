/*
Author:
Works with MySQL only
script for bombarding specific tables of the database
prepare phase: Consists 2 phases: the first phase copies data from a specified table from the db and stores it in a binary file in the disk. The second phase copies data from the specified table and creates a temporary table out of it. These 2 phases happen parallely
run phase: the run phase bombards the temporary table with the data

The TEMP_TABLE_PREP_SIZE_RATIO environment variable dictates the percentage rows of the original table are going to be copied to the new temporary table
*/

/*
options:
host: db host
user: username
password*: password if to be given on the command line
ask-pass: ask for the password at runtime
database: name of the database
tablename: name of the table
prepare: --prepare for prep stage
run: --run for run stage
*/

package main

import (
	"bufio"
	"database/sql"
	"flag"
	"fmt"
	"math"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/golang/glog"
)

// signals for channels
type cSignal struct {
	FATAL string
	AOK   string
	WARN  string
}

func (c cSignal) fatal() string {
	return "FATAL"
}

func (c cSignal) aok() string {
	return "AOK"
}

func (c cSignal) warn() string {
	return "WARNING"
}

/*
database errors
*/
type dbError struct {
	code int // mysql database error code
	msg  string
}

func (e dbError) Error() string {
	return fmt.Sprintf("DB ERROR CODE: %d \n MSG: %s", e.code, e.msg)
}

/*
Query defines the query itself, along with wait_time in milliseconds and error in int
*/
type Query struct {
	query string
	wt    int
}

/*
QueryBatch defines the batch of queries to be executed
q is the query defined, sleep is the sleep time between queries in milliseconds, size is the number of queries in the batch
*/
type QueryBatch struct {
	q     []Query
	db    *sql.DB
	sleep int
	size  int
}

func (q Query) executeRead(db *sql.DB) *sql.Rows {
	st := time.Now()
	rows, err := db.Query(q.query)
	et := time.Now()
	q.wt = int(math.Round(et.Sub(st).Seconds() * 1000))
	if err != nil {
		glog.Fatal(err)
	}
	defer rows.Close()
	// as long as there is an open result set(represented by rows), the underlying connection is busy and can't be used for any other query
	// That means it is not available in the connection pool. If you iterate over all the rows with rows.Next(), eventually you'll read the last row and rows.Next()
	// will encounter an internal EOF call and call rows.Close(). But if for some reason rows.Close() is not called and we exit the function, not defering rows.Close()
	// can become a potential source of memory leak in that case. In case of an error however rows.Close() is called internally
	return rows
}

func (q Query) executeReadAsync(db *sql.DB, rowChan chan *sql.Rows, wg sync.WaitGroup) {
	defer wg.Done()
	st := time.Now()
	rows, err := db.Query(q.query)
	et := time.Now()
	q.wt = int(math.Round(et.Sub(st).Seconds() * 1000))
	var cSignal cSignal
	if err != nil {
		glog.Info(err)
		return
	}
	defer rows.Close()
	rowChan <- rows
}

func (q Query) executeWrite(db *sql.DB) {
	st := time.Now()
	_, err := db.Exec(q.query)
	et := time.Now()
	if err != nil {
		glog.Fatal(err)
	}
	q.wt = int(math.Round(et.Sub(st).Seconds() * 1000))
}

func (q Query) executeWriteAsync(db *sql.DB, wg sync.WaitGroup) {
	defer wg.Done()
	st := time.Now()
	_, err := db.Exec(q.query)
	et := time.Now()
	var cSignal cSignal
	if err != nil {
		glog.Info(err)
		return
	}
	q.wt = int(math.Round(et.Sub(st).Seconds() * 1000))
}

func writeRowsToDisk(rows *sql.Rows, f *os.File, wg sync.WaitGroup) {
	defer wg.Done()
	cols, err := rows.Columns()
	// skipping the id field
	idPos := 0
	for i, col := range cols { // go does not have a straightforward way to get an element
		if col == "id" {
			idPos = i
		}
	}
	cols = append(cols[:idPos], cols[idPos+1:]...)
	/*
		Now, when calling a function, ... does the opposite: it unpacks a slice and passes them as separate arguments to a variadic function.
	*/
	if err != nil {
		glog.Info(err)
		return
	}

	rawResult := make([][]byte, len(cols))

	dest := make([]interface{}, len(cols))

	for i, _ := range rawResult {
		dest[i] = &rawResult[i]
	}

	for rows.Next() {
		err = rows.Scan(dest...) // check out variadic functions in go
		/*
			Note to programmer: in python, variadic functions can be written at some level using *args and **kwargs. In go, variadic functions are typically written with `...`
			Recall that Scan takes a slice of interfaces using `...`. Here we can pass that interface using `...` and as long as we have pointer
		*/
		if err != nil {
			glog.Info(err)
			return
		}

		for _, raw := range rawResult {
			if raw == nil {
				f.Write([]byte("NULL"))
			} else {
				f.Write(raw)
			}
		}
	}
}

func getConnection(host string, user string, pwd string, db string, port int) *sql.DB {
	// ping the database to see if it can connect
	conn, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", user, pwd, host, port, db))
	if err != nil {
		glog.Fatal(err) // err is actually an interface
	}
	return conn
}

func prepare(db *sql.DB, table string, pr float64) {
	/*
		prepare creates a new temporary table using the same schema as the specified table and copies `pr` amount of data to it
	*/
	var count int
	row := db.QueryRow("SELECT COUNT(1) FROM %s", table)
	err := row.Scan(&count)
	if err != nil {
		glog.Fatal(err)
	}
	prepN := math.Round(pr * float64(count))
	//prepN is now the number of rows to be copied to the temporary table and copied to disk

}

func main() {

	tempTablePrepSizeRatio, _ := strconv.ParseFloat(os.Getenv("TEMP_TABLE_PREP_SIZE_RATIO"), 32) // the ratio of the temp table size to the actual table size, this amount of data is copied
	// to the temporary table from the new table
	if tempTablePrepSizeRatio == 0.00 {
		defVal := 0.66
		glog.Warning("TEMP_TABLE_PREP_SIZE_RATIO has not been set, defaulting to %0.2f", defVal)
		tempTablePrepSizeRatio = defVal
	}
	host := flag.String("host", "localhost", "hostname of the database")
	user := flag.String("username", "root", "username")
	pwd := flag.String("password", "toor", "password")
	port := flag.Int("port", 3306, "port")
	db := flag.String("database", "ilapahsi", "database to execute on")
	table := flag.String("tablename", "ilapahsi", "tablename")
	prep := flag.Bool("prepare", false, "if prepare")
	run := flag.Bool("run", false, "if run")
	verbose := flag.Bool("verbose", false, "verbose logging")
	dry := flag.Bool("dry", false, "dry run")
	cpm := flag.Int("cpm", 10, "calls per minute on the table")

	if *prep {
		conn := getConnection(*host, *user, *pwd, *db, *port)
		glog.Info("Starting prepare phase for table: %s", *table)
		prepare(conn, *table, tempTablePrepSizeRatio)
	}
}
