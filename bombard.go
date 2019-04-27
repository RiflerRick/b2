/*
Author:
Works with MySQL only
script for bombarding specific tables of the database
prepare phase: This phase copies data from the specified table and creates a temporary table out of it.
run phase: the run phase bombards the temporary table with the data

The TEMP_TABLE_SIZE_RATIO environment variable dictates the percentage rows of the original table are going to be copied to the new temporary table. These are the rows that are going to be used for bombarding.
In a way this is considered to be the recent data being used

-stderrthreshold=INFO is required
-v=2 for 2 level verbosity
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
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
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
	query     string
	queryType string
	wt        int
}

/*
Transactions for db transactions
*/
type Transaction struct {
	transaction *sql.Tx
	wt          int
}

/*
Feasible Query defines a query that can be used to hit the original table during benchmakring
*/
type FeasibleQuery interface {
	getQueryType() string
	getQuery() string
	getWaitTime() int
	executeRead(db *sql.DB) *sql.Rows
	executeReadRow(db *sql.DB) *sql.Row
	executeReadAsync(db *sql.DB, rowChan chan *sql.Rows)
	executeReadRowAsync(db *sql.DB, rowChan chan *sql.Row)
	executeWrite(db *sql.DB)
	executeWriteAsync(db *sql.DB)
}

func (q Query) executeRead(db *sql.DB) *sql.Rows {
	st := time.Now()
	rows, err := db.Query(q.query)
	et := time.Now()
	q.wt = int(math.Round(et.Sub(st).Seconds() * 1000))
	if err != nil {
		glog.Fatal(err)
	}
	// as long as there is an open result set(represented by rows), the underlying connection is busy and can't be used for any other query
	// That means it is not available in the connection pool. If you iterate over all the rows with rows.Next(), eventually you'll read the last row and rows.Next()
	// will encounter an internal EOF call and call rows.Close(). But if for some reason rows.Close() is not called and we exit the function, not defering rows.Close()
	// can become a potential source of memory leak in that case. In case of an error however rows.Close() is called internally
	return rows
}

func (q Query) executeReadRow(db *sql.DB) *sql.Row {
	st := time.Now()
	row := db.QueryRow(q.query)
	et := time.Now()
	q.wt = int(math.Round(et.Sub(st).Seconds() * 1000))
	return row
}

func (q Query) executeReadAsync(db *sql.DB, rowChan chan *sql.Rows) {
	st := time.Now()
	rows, err := db.Query(q.query)
	et := time.Now()
	q.wt = int(math.Round(et.Sub(st).Seconds() * 1000))
	if err != nil {
		glog.Info(err)
		return
	}
	rowChan <- rows
}

func (q Query) executeReadRowAsync(db *sql.DB, rowChan chan *sql.Row) {
	st := time.Now()
	row := db.QueryRow(q.query)
	et := time.Now()
	q.wt = int(math.Round(et.Sub(st).Seconds() * 1000))
	rowChan <- row
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

func (t Transaction) commit() {
	st := time.Now()
	err := t.transaction.Commit()
	if err != nil {
		glog.Fatal(err)
	}
	et := time.Now()
	t.wt = int(math.Round(et.Sub(st).Seconds() * 1000))
}

func (t Transaction) executeVariadic(query string, data ...interface{}) {
	_, err := t.transaction.Exec(query, data...)
	if err != nil {
		glog.Fatal(err)
	}
}

func (q Query) executeWriteAsync(db *sql.DB) {
	st := time.Now()
	_, err := db.Exec(q.query)
	et := time.Now()
	if err != nil {
		glog.Info(err)
		return
	}
	q.wt = int(math.Round(et.Sub(st).Seconds() * 1000))
}

/*
	function to publish data to the bus after reading from the source db
	to be called as a go routine. publishes data to the bus channel to be consumed by bombarding routines
*/
func publishToBus(db *sql.DB, table string, startID int, count int, timeToRun int, readChunkSize *int, startTime *time.Time, bus chan *sql.Rows) {

	source := rand.NewSource(time.Now().UnixNano())
	r := rand.New(source)
	for true {
		timeNow := time.Now()
		if int(math.Round(timeNow.Sub(*startTime).Minutes())) > timeToRun {
			break
		}
		offset := r.Intn(count)
		rows, err := db.Query(fmt.Sprintf("SELECT * FROM %s WHERE id >= %d LIMIT %d OFFSET %d", table, startID, *readChunkSize, offset))
		if err != nil {
			glog.Info(err)
			return
		}
		bus <- rows
	}
}

func getRunChunk(db *sql.DB, table string, runN int, prepN int) (int, int) {
	var startID int
	var endID int
	var count int
	err := db.QueryRow(fmt.Sprintf("SELECT id FROM %s ORDER BY ID DESC LIMIT 1 OFFSET %d", table, prepN)).Scan(&startID)
	if err != nil {
		glog.Fatal(err)
	}
	err = db.QueryRow(fmt.Sprintf("SELECT id FROM %s ORDER BY ID DESC LIMIT 1 OFFSET %d", table, 1)).Scan(&endID)
	if err != nil {
		glog.Fatal(err)
	}
	err = db.QueryRow(fmt.Sprintf("SELECT COUNT(1) FROM %s WHERE id >= %d and i < %d", table, startID, endID)).Scan(&count)
	return startID, count
}

func getSubset(colSelect map[string]bool) {
	for k := range colSelect {
		if rand.Intn(2) == 1 {
			colSelect[k] = true
		} else {
			colSelect[k] = false
		}
	}

}

func getQuery(queryType string, tableName string, colData map[string]interface{}, indexedCols map[string]bool, allowMissingIndex map[string]bool) [2]interface{} {
	// insert and update are dependent on delete queries, for each insert and update a corresponding
	// delete must be fired first

	// the first string returned is the actual query, the second one is the dependent query
	queries := [2]interface{}{nil, nil}
	dependentQueryHit := false
	var colSelect map[string]bool
	for k := range colData {
		colSelect[k] = false
	}
	for true {
		if queryType == "read" {
			getSubset(colSelect)
			baseQuery := fmt.Sprintf("SELECT * FROM %s WHERE ", tableName)
			for k, v := range colData {
				if allowMissingIndex["read"] {
					if colSelect[k] {
						baseQuery += fmt.Sprintf("%s = %s and ", k, v)
					}
				} else {
					if colSelect[k] && indexedCols[k] {
						baseQuery += fmt.Sprintf("%s = %s and ", k, v)
					}
				}
			}
			baseQuery = strings.TrimSuffix(baseQuery, " and ")
			queries[0] = baseQuery
			break
		} else if queryType == "create" {
			baseQuery := fmt.Sprintf("INSERT INTO %s VALUES (")
			for _, v := range colData {
				baseQuery += fmt.Sprintf("%s, ", v)
			}
			baseQuery = strings.TrimSuffix(baseQuery, ",")
			baseQuery += fmt.Sprintf(")")
			queryType = "delete"
			queries[0] = baseQuery
			dependentQueryHit = true
		} else if queryType == "update" {
			baseQuery := fmt.Sprintf("UPDATE %s SET ")
			getSubset(colSelect)
			for k, v := range colData {
				if allowMissingIndex["update"] {
					if colSelect[k] {
						baseQuery += fmt.Sprintf("%s = %s,", k, v)
					}
				} else {
					if colSelect[k] && indexedCols[k] {
						baseQuery += fmt.Sprintf("%s = %s,", k, v)
					}
				}
			}
			baseQuery = strings.TrimSuffix(baseQuery, ",") + " WHERE "
			getSubset(colSelect)
			for k, v := range colData {
				if allowMissingIndex["update"] {
					if colSelect[k] {
						baseQuery += fmt.Sprintf("%s = %s and ", k, v)
					}
				} else {
					if colSelect[k] && indexedCols[k] {
						baseQuery += fmt.Sprintf("%s = %s and ", k, v)
					}
				}
			}
			baseQuery = strings.TrimSuffix(baseQuery, " and ")
			queryType = "delete"
			queries[0] = baseQuery
			dependentQueryHit = true
		} else {
			baseQuery := fmt.Sprintf("DELETE FROM %s ", tableName)
			if !dependentQueryHit {
				getSubset(colSelect)
			}
			for k, v := range colData {
				if allowMissingIndex["read"] {
					if colSelect[k] {
						baseQuery += fmt.Sprintf("%s = %s and ", k, v)
					}
				} else {
					if colSelect[k] && indexedCols[k] {
						baseQuery += fmt.Sprintf("%s = %s and ", k, v)
					}
				}
			}
			baseQuery = strings.TrimSuffix(baseQuery, " and ")
			if dependentQueryHit {
				queries[1] = baseQuery
			} else {
				queries[0] = baseQuery
			}
			break
		}
	}
	return queries
}

/*
subscribe to bus for hitting the db. sleep decides how much to sleep in between queries
queryTypeCPM: map storing the CPM values for each query. The type of query to be fired will be chosen by this CPM
*/
func bombard(queryType string, tableName string, db *sql.DB, bus chan *sql.Rows, sleepTime int, indexedCols map[string]bool, allowMissingIndex map[string]bool, qWT chan int, busEmpty chan string) {
	var r *sql.Rows
	var q Query
	for {
		select {
		case r = <-bus:
			cols, _ := r.Columns()
			for r.Next() {
				columns := make([]interface{}, len(cols))
				columnPointers := make([]interface{}, len(cols))
				for i := range columns {
					columnPointers[i] = &columns[i]
				}
				err := r.Scan(columnPointers...)
				if err != nil {
					glog.Info(err)
					return
				}
				colData := make(map[string]interface{})
				for i, colName := range cols {
					val := columnPointers[i].(*interface{})
					colData[colName] = *val
				}
				if queryType == "select" {
					query := getQuery(queryType, tableName, colData, indexedCols, allowMissingIndex)
					if queryType == "select" || queryType == "delete" {
						// no dependent query
						q.query, _ = query[0].(string) // type assertion for string type
						if queryType == "select" {
							q.executeRead(db)
						} else {
							q.executeWrite(db)
						}
						qWT <- q.wt
						time.Sleep(time.Millisecond * time.Duration(sleepTime))
					} else {
						q.query, _ = query[1].(string)
						q.executeWrite(db)
						qWT <- q.wt
						q.query, _ = query[0].(string)
						q.executeWrite(db)
						qWT <- q.wt
						time.Sleep(time.Millisecond * time.Duration(sleepTime))
					}
				}
			}
		default:
			busEmpty <- queryType
			break
		}

	}
}

func writeRowsToTempTable(expDb *sql.DB, tempTableName string, rows *sql.Rows, wg *sync.WaitGroup, insertCommitsAfter int) {
	defer wg.Done()
	defer rows.Close()
	cols, err := rows.Columns()
	/*
		Now, when calling a function, ... does the opposite: it unpacks a slice and passes them as separate arguments to a variadic function.
	*/
	if err != nil {
		glog.Info(err)
		return
	}

	colData := make([]interface{}, 0)
	var tx Transaction
	startTrx := true
	var baseQuery string
	rowsFetched := 0

	for rows.Next() {
		if startTrx {
			colData = make([]interface{}, 0)
			startTrx = false
			tx.transaction, err = expDb.Begin()
			if err != nil {
				glog.Fatal(err)
			}
			baseQuery = fmt.Sprintf("INSERT INTO %s VALUES ", tempTableName)
		}

		columns := make([]interface{}, len(cols))
		columnPointers := make([]interface{}, len(cols))
		for i := range columns {
			columnPointers[i] = &columns[i]
		}

		err = rows.Scan(columnPointers...) // check out variadic functions in go
		rowsFetched++
		/*
			Note to programmer: in python, variadic functions can be written at some level using *args and **kwargs. In go, variadic functions are typically written with `...`
			Recall that Scan takes a slice of interfaces using `...`. Here we can pass that interface using `...`
		*/
		if err != nil {
			glog.Info(err)
			return
		}
		for i := range cols {
			val := columnPointers[i].(*interface{})
			// if *val == nil {
			// 	colData = append(colData, "NULL")
			// 	continue
			// }
			colData = append(colData, *val)
		}

		for i := range cols {
			if i == 0 {
				baseQuery += "("
			} else if i == (len(cols) - 1) {
				baseQuery += "?"
				baseQuery += "),"
				break
			}
			baseQuery += "?,"
		}
		if rowsFetched%insertCommitsAfter == 0 {
			baseQuery = strings.TrimSuffix(baseQuery, ",")
			tx.executeVariadic(baseQuery, colData...)
			tx.commit()
			startTrx = true
		}
	}
	if rowsFetched%insertCommitsAfter != 0 {
		baseQuery = strings.TrimSuffix(baseQuery, ",")
		tx.executeVariadic(baseQuery, colData...) // to handle the last bit
		tx.commit()
	}
}

func getConnection(host string, user string, pwd string, db string, port int) *sql.DB {
	// ping the database to see if it can connect
	conn, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", user, pwd, host, port, db))
	if err != nil {
		glog.Fatal(err) // err is actually an interface
	}
	return conn
}

// copy prepN amount of data and dump into a temporary table. copying is done in batches
func chunkCopyDataTempTable(db *sql.DB, expDb *sql.DB, table string, prepN int, prepareChunkSize int, insertCommitsAfter int) {
	tempTableName := table + "_c4"
	_, err := expDb.Exec(fmt.Sprintf("CREATE TABLE %s LIKE %s", tempTableName, table))
	if err != nil {
		glog.Fatal(err)
	}
	glog.V(0).Infof("Table %s has been created", tempTableName)
	var startID int
	var endID int
	err = db.QueryRow(fmt.Sprintf("SELECT id FROM %s ORDER BY ID DESC LIMIT 1 OFFSET %d", table, prepN)).Scan(&startID)
	if err != nil {
		glog.Fatal(err)
	}
	err = db.QueryRow(fmt.Sprintf("SELECT id FROM %s ORDER BY ID DESC LIMIT 1", table)).Scan(&endID)
	if err != nil {
		glog.Fatal(err)
	}
	var r *sql.Row

	var chanSize int
	if prepN%prepareChunkSize != 0 {
		chanSize = prepN/prepareChunkSize + 1
	} else {
		chanSize = prepN / prepareChunkSize
	}

	rowData := make(chan *sql.Rows, chanSize) //buffered channels is one way of combining waitgroups and channels

	j := startID
	breakLoop := false
	numSelected := 0
	goRoutinesSpun := 0
	for {
		glog.V(2).Infof("Selecting rows from %s starting with id: %d", table, j)
		var selQuery Query
		var getIDQuery Query
		selQuery.query = fmt.Sprintf("SELECT * FROM %s WHERE id >= %d ORDER BY id asc LIMIT %d", table, j, prepareChunkSize)
		glog.V(3).Infof("Bulk select query: %s", selQuery.query)

		goRoutinesSpun++
		glog.V(2).Infof("spawning go routine %d for selecting rows from id: %d for a chunk of %d", goRoutinesSpun, j, prepareChunkSize)
		go selQuery.executeReadAsync(db, rowData)

		numSelected += prepareChunkSize

		if breakLoop {
			break
		}

		getIDQuery.query = fmt.Sprintf("SELECT id FROM %s WHERE id > %d ORDER BY id asc LIMIT 1 OFFSET %d", table, j, prepareChunkSize-1)
		glog.V(3).Infof("id select query: %s", getIDQuery.query)
		r = getIDQuery.executeReadRow(db)
		err = r.Scan(&j)
		if err != nil {
			glog.Fatal(err)
		}
		if prepN-numSelected <= prepareChunkSize {
			// to address the last chunk
			prepareChunkSize = prepN - numSelected
			breakLoop = true
		}
	}
	glog.V(1).Infof("bulk select go routines spun: %d", goRoutinesSpun)
	var wg sync.WaitGroup
	goRoutinesSpun = 0
	for i := 0; i < chanSize; i++ {
		// this is not the best way, for select may be a better way
		wg.Add(1)
		goRoutinesSpun++
		glog.V(2).Infof("Spanwing go routine %d for inserting data!!!", goRoutinesSpun)
		go writeRowsToTempTable(expDb, tempTableName, <-rowData, &wg, insertCommitsAfter)
	}
	glog.V(1).Infof("Waiting for insert routines to finish!!!")
	wg.Wait() // waiting for all writes to finish
}

func prepare(db *sql.DB, expDb *sql.DB, table string, pr float64, prepareChunkSize int, insertCommitsAfter int) {
	/*
		prepare creates a new temporary table using the same schema as the specified table and copies `pr` amount of data to it
	*/
	var count int
	err := db.QueryRow(fmt.Sprintf("SELECT COUNT(1) FROM %s", table)).Scan(&count)
	if err != nil {
		glog.Fatal(err)
	}
	prepN := int(math.Round(pr * float64(count)))
	glog.V(2).Infof("prepN has been set as %d", prepN)
	glog.V(2).Infof("number of rows in the table %s: %d", table, count)
	chunkCopyDataTempTable(db, expDb, table, prepN, prepareChunkSize, insertCommitsAfter)
}

func main() {

	insertCommitsAfter, _ := strconv.ParseInt(os.Getenv("INSERT_COMMITS_AFTER"), 10, 0)
	prepPhaseChunkSize, _ := strconv.ParseInt(os.Getenv("PREP_PHASE_CHUNK_SIZE"), 10, 0)
	tempTablePrepSizeRatio, _ := strconv.ParseFloat(os.Getenv("TEMP_TABLE_SIZE_RATIO"), 32) // the ratio of the temp table size to the actual table size, this amount of data is copied
	// to the temporary table from the new table

	if insertCommitsAfter == 0 {
		defVal := 1000
		glog.V(0).Infof("INSERT_COMMITS_AFTER has not been set, defaulting to %d", defVal)
		insertCommitsAfter = int64(defVal)
	}
	if tempTablePrepSizeRatio == 0.00 {
		defVal := 0.66
		glog.V(0).Infof("TEMP_TABLE_SIZE_RATIO has not been set, defaulting to %0.2f", defVal)
		tempTablePrepSizeRatio = defVal
	}
	if prepPhaseChunkSize == 0 {
		defVal := 10000
		glog.V(0).Infof("PREP_PHASE_CHUNK_SIZE has not been set, defaulting to %d", defVal)
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
	createCPM := flag.Int("create-cpm", 0, "desired insert calls per minute on the table")
	readCPM := flag.Int("read-cpm", 0, "desired select calls per minute on the table")
	updateCPM := flag.Int("update-cpm", 0, "desired update calls per minute on the table")
	deleteCPM := flag.Int("delete-cpm", 0, "desired delete calls per minute on the table")
	allowMissingIndexRead := flag.Bool("allow-missing-index-reads", false, "allows missing index reads if true")
	allowMissingIndexUpdate := flag.Bool("allow-missing-index-updates", false, "allows missing index updates if true")
	allowMissingIndexDelete := flag.Bool("allow-missing-index-deletes", false, "allows missing index deletes if true")
	time := flag.Int("time", 10, "time in minutes to bombard")

	flag.Parse()

	if *askPass {
		// TODO: check how we can effectively ask for the password without displaying
		reader := bufio.NewReader(os.Stdin)
		fmt.Println("Db password: ")
		text, _ := reader.ReadString('\n')
		pwd = &text
	}
	if *prep {
		glog.V(0).Info("Running 'prep' phase!!!")
		conn := getConnection(*host, *user, *pwd, *db, *port)
		expConn := getConnection(*expHost, *user, *pwd, *db, *port)
		glog.V(0).Infof("Starting prepare phase for table: %s", *table)
		prepare(conn, expConn, *table, tempTablePrepSizeRatio, int(prepPhaseChunkSize), int(insertCommitsAfter))
	} else if *run {
		glog.V(0).Info("Running 'run' phase!!!")
		// TODO
	} else {
		glog.V(0).Info("Neither prep nor run passed. Aborting!!!")
	}
}
