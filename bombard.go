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
	// Loading everything into memory is not an ideal way of going about this
	// TODO: change this
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

// copy prepN amount of data and dump into a temporary table. copying is done in batches
func chunkCopyDataTempTable(db *sql.DB, table string, prepN int, prepareChunkSize int, w sync.WaitGroup) {
	defer w.Done()
	var cTempTableCopy QueryBatch
	cTempTableCopy.size = prepN
	cTempTableCopy.db = db
	_, err := db.Exec("CREATE TABLE %s LIKE %s", table+"_c4", table)
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
	for i := prepN; i > 0; i -= prepareChunkSize {
		j = id - (c * prepareChunkSize)
		c++
		var query Query
		query.query = fmt.Sprintf("INSERT INTO %s SELECT * FROM %s WHERE id <= %d ORDER BY id desc LIMIT %s", table+"_c4", table, j, prepareChunkSize)
		cTempTableCopy.q = append(cTempTableCopy.q, query)
		wg.Add(1)
		go query.executeWriteAsync(db, wg)
	}
	wg.Wait() // waiting for all go routines to finish
}

func chunkCopyDataDisk(db *sql.DB, table string, prepN int, prepareChunkSize int, w sync.WaitGroup) {
	defer w.Done()
	var cDiskCopy QueryBatch
	cDiskCopy.size = prepN
	cDiskCopy.db = db
	var id int
	err := db.QueryRow("SELECT id FROM %s ORDER BY ID DESC LIMIT %s,1", table, prepN).Scan(&id)
	if err != nil {
		glog.Fatal(err)
	}
	var rowData chan *sql.Rows
	var wg sync.WaitGroup
	c := 0
	var j int
	for i := prepN; i > 0; i -= prepareChunkSize {
		j = id + (c * prepareChunkSize)
		c++
		var query Query
		query.query = fmt.Sprintf("SELECT * FROM %s WHERE id >= %d ORDER BY id asc LIMIT %d", table+"_c4", table, j, prepareChunkSize)
		cDiskCopy.q = append(cDiskCopy.q, query)
		wg.Add(1)
		go query.executeReadAsync(db, rowData, wg)
	}
	wg.Wait() // waiting for all go routines to finish
	for {     // since wait is already done, here we get all rows using nonblocking channel reception
		select {
		case rows := <-rowData:
			// TODO
		default:
			break
		}
	}
}

func prepare(db *sql.DB, table string, pr float64, prepareChunkSize int) {
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
	wg.Add(2)
	go chunkCopyDataDisk(db, table, prepN, prepareChunkSize, wg)
	go chunkCopyDataTempTable(db, table, prepN, prepareChunkSize, wg)
	wg.Wait()
}

func main() {

	prepPhaseChunkSize, _ := strconv.ParseInt(os.Getenv("PREP_PHASE_CHUNK_SIZE"), 10, 0)
	tempTablePrepSizeRatio, _ := strconv.ParseFloat(os.Getenv("TEMP_TABLE_PREP_SIZE_RATIO"), 32) // the ratio of the temp table size to the actual table size, this amount of data is copied
	// to the temporary table from the new table

	if tempTablePrepSizeRatio == 0.00 {
		defVal := 0.66
		glog.Warning("TEMP_TABLE_PREP_SIZE_RATIO has not been set, defaulting to %0.2f", defVal)
		tempTablePrepSizeRatio = defVal
	}
	if prepPhaseChunkSize == 0 {
		defVal := 10000
		glog.Warning("PREP_PHASE_CHUNK_SIZE has not been set, defaultinig to %d", defVal)
		prepPhaseChunkSize = int64(defVal)
	}
	host := flag.String("host", "localhost", "hostname of the database")
	user := flag.String("username", "root", "username")
	pwd := flag.String("password", "toor", "password")
	port := flag.Int("port", 3306, "port")
	db := flag.String("database", "ilapahsi", "database to execute on")
	askPass := flag.Bool("ask-pass", false, "Ask Pass")
	table := flag.String("tablename", "ilapahsi", "tablename")
	prep := flag.Bool("prepare", false, "if prepare")
	run := flag.Bool("run", false, "if run")
	verbose := flag.Bool("verbose", false, "verbose logging")
	dry := flag.Bool("dry", false, "dry run")
	cpm := flag.Int("cpm", 10, "calls per minute on the table")

	flag.Parse()

	if *askPass {
		reader := bufio.NewReader(os.Stdin)
		fmt.Println("Db password: ")
		text, _ := reader.ReadString('\n')
		fmt.Println(text)
	}

	if *prep {
		conn := getConnection(*host, *user, *pwd, *db, *port)
		glog.Info("Starting prepare phase for table: %s", *table)
		prepare(conn, *table, tempTablePrepSizeRatio, int(prepPhaseChunkSize))
	}
}
