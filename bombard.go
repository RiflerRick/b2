/*
Author:
Works with MySQL only
script for bombarding specific tables of the database
prepare phase: This phase copies data from the specified table and creates a temporary table out of it.
run phase: the run phase bombards the temporary table with the data

The TEMP_TABLE_SIZE_RATIO environment variable dictates the percentage rows of the original table are going to be copied to the new temporary table. These are the rows that are going to be used for bombarding.
In a way this is considered to be the recent data being used
*/

/*
options:
host: db host
expHost: experiment db host. The host db to bombard, this is useful in case of minimizing impact
on the db having the original table
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
	if err != nil {
		glog.Info(err)
		return
	}
	q.wt = int(math.Round(et.Sub(st).Seconds() * 1000))
}

func writeRowsToTempTable(expDb *sql.DB, tempTableName string, rows *sql.Rows, wg sync.WaitGroup) {
	defer wg.Done()
	cols, err := rows.Columns()
	/*
		Now, when calling a function, ... does the opposite: it unpacks a slice and passes them as separate arguments to a variadic function.
	*/
	if err != nil {
		glog.Info(err)
		return
	}
	// rawResult := make([][]byte, len(cols))
	dest := make([]interface{}, len(cols))

	// point all interface
	// for i := range rawResult {
	// 	dest[i] = &rawResult[i]
	// }
	var query Query
	baseQuery := fmt.Sprintf("INSERT INTO %s VALUES ", tempTableName)
	for i := range cols {
		if i == 0 {
			baseQuery += "("
		} else if i == (len(cols) - 1) {
			baseQuery += "%s"
			baseQuery += ")"
			break
		}
		baseQuery += "%s,"
	}
	for rows.Next() {
		err = rows.Scan(dest...) // check out variadic functions in go
		/*
			Note to programmer: in python, variadic functions can be written at some level using *args and **kwargs. In go, variadic functions are typically written with `...`
			Recall that Scan takes a slice of interfaces using `...`. Here we can pass that interface using `...`
		*/
		if err != nil {
			glog.Info(err)
			return
		}
		query.query = fmt.Sprintf(baseQuery, dest...)
		query.executeWrite(expDb)
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

// copy prepN amount of data and dump into a temporary table. copying is done in batches
func chunkCopyDataTempTable(db *sql.DB, expDb *sql.DB, table string, prepN int, prepareChunkSize int, w sync.WaitGroup) {
	defer w.Done()
	var cTempTableCopy QueryBatch
	cTempTableCopy.size = prepN
	cTempTableCopy.db = expDb
	tempTableName := table + "_c4"
	_, err := expDb.Exec("CREATE TABLE %s LIKE %s", tempTableName, table)
	if err != nil {
		glog.Fatal(err)
	}
	var id int
	err = db.QueryRow("SELECT id FROM %s ORDER BY ID DESC LIMIT 1", table).Scan(&id)
	if err != nil {
		glog.Fatal(err)
	}
	var wg sync.WaitGroup
	c := 0
	var j int
	var rowData chan *sql.Rows
	for i := prepN; i > 0; i -= prepareChunkSize {
		j = id - (c * prepareChunkSize)
		c++
		var query Query
		query.query = fmt.Sprintf("SELECT * FROM %s WHERE id <= %d ORDER BY id desc LIMIT %d", table, j, prepareChunkSize)
		cTempTableCopy.q = append(cTempTableCopy.q, query)
		wg.Add(1)
		go query.executeReadAsync(db, rowData, wg)
	}
	wg.Wait() // waiting for all go routines to finish
	for {     // since wait is already done, here we get all rows using nonblocking channel reception
		select {
		case rows := <-rowData:
			wg.Add(1)
			go writeRowsToTempTable(expDb, tempTableName, rows, wg)
		default:
			break
		}
	}
	wg.Wait() // waiting for all writes to finish
}

func prepare(db *sql.DB, expDb *sql.DB, table string, pr float64, prepareChunkSize int) {
	/*
		prepare creates a new temporary table using the same schema as the specified table and copies `pr` amount of data to it
	*/
	var count int
	err := db.QueryRow("SELECT COUNT(1) FROM %s", table).Scan(&count)
	if err != nil {
		glog.Fatal(err)
	}
	prepN := int(math.Round(pr * float64(count)))
	//prepN is now the number of rows to be copied(from the end) to the temporary table and copied to disk
	var wg sync.WaitGroup
	wg.Add(1)
	go chunkCopyDataTempTable(db, expDb, table, prepN, prepareChunkSize, wg)
	wg.Wait()
}

func main() {

	prepPhaseChunkSize, _ := strconv.ParseInt(os.Getenv("PREP_PHASE_CHUNK_SIZE"), 10, 0)
	tempTablePrepSizeRatio, _ := strconv.ParseFloat(os.Getenv("TEMP_TABLE_SIZE_RATIO"), 32) // the ratio of the temp table size to the actual table size, this amount of data is copied
	// to the temporary table from the new table

	if tempTablePrepSizeRatio == 0.00 {
		defVal := 0.66
		glog.Warning(fmt.Sprintf("TEMP_TABLE_SIZE_RATIO has not been set, defaulting to %0.2f", defVal))
		tempTablePrepSizeRatio = defVal
	}
	if prepPhaseChunkSize == 0 {
		defVal := 10000
		glog.Warning(fmt.Sprintf("PREP_PHASE_CHUNK_SIZE has not been set, defaultinig to %d", defVal))
		prepPhaseChunkSize = int64(defVal)
	}
	host := flag.String("host", "localhost", "hostname of the database")
	expHost := flag.String("experiment-host", "localhost", "In case the benchmarking is being done in a separate host from where the actual table is present in, use this flag")
	user := flag.String("username", "root", "username")
	pwd := flag.String("password", "toor", "password")
	port := flag.Int("port", 3306, "port")
	db := flag.String("database", "ilapahsi", "database to execute on")
	askPass := flag.Bool("ask-pass", false, "Ask Pass")
	table := flag.String("tablename", "ilapahsi", "tablename")
	prep := flag.Bool("prepare", false, "if prepare")
	run := flag.Bool("run", false, "if run")
	// verbose := flag.Bool("verbose", false, "verbose logging")
	// dry := flag.Bool("dry", false, "dry run")
	// cpm := flag.Int("cpm", 10, "calls per minute on the table")

	flag.Parse()

	if *askPass {
		reader := bufio.NewReader(os.Stdin)
		fmt.Println("Db password: ")
		text, _ := reader.ReadString('\n')
		pwd = &text
	}
	if *prep {
		glog.Info("Running 'prep' phase!!!")
		conn := getConnection(*host, *user, *pwd, *db, *port)
		expConn := getConnection(*expHost, *user, *pwd, *db, *port)
		glog.Info(fmt.Sprintf("Starting prepare phase for table: %s", *table))
		prepare(conn, expConn, *table, tempTablePrepSizeRatio, int(prepPhaseChunkSize))
	} else if *run {
		glog.Info("Running 'run' phase!!!")
		// TODO
	} else {
		glog.Info("Neither prep nor run passed. Aborting!!!")
	}
}
