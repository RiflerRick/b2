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
	"os"
	"strconv"
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
dM: desiredMetadata consisting of CPM, wT //this never changes
rM: runMetadata consisting of CPM, wT (actual)
cM: controllerMetadata consisting of instances_running, sleepTime and chinkSize
*/

/*
MasterPublishController
*/
type MasterPublishController struct {
	cM map[string]interface{}
}

/*
MasterSubscribeController
*/
type MasterSubscribeController struct {
	cM map[string]interface{}
}

/*
Controller can either be the MasterPublishController or MasterSubscribeController
*/
type Controller interface {
	upscale(queryType *string, dM map[string]interface{}, rM map[string]interface{}, cM map[string]interface{}, dontCare *bool, scale chan bool)
	downscale(queryType *string, dM map[string]interface{}, rM map[string]interface{}, cM map[string]interface{}, dontCare *bool, scale chan bool)
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

func (mpc MasterPublishController) upscale(queryType *string, dM map[string]interface{}, rM map[string]interface{}, cM map[string]interface{}, dontCare *bool, scale chan bool) {
	if *dontCare {
		cM["instances"].(map[string]interface{})[*queryType] = cM["instances"].(map[string]interface{})[*queryType].(int) + 1
		scale <- true
	}
}

func (msc MasterSubscribeController) upscale(queryType *string, dM map[string]interface{}, rM map[string]interface{}, cM map[string]interface{}, dontCare *bool, scale chan bool) {
	if rM["CPM"].(map[string]interface{})[*queryType].(int) > dM["CPM"].(map[string]interface{})[*queryType].(int) {
		cM["instances"].(map[string]interface{})[*queryType] = cM["instances"].(map[string]interface{})[*queryType].(int) + 1
		scale <- true
	}
}

func (mpc MasterPublishController) downscale(queryType *string, dM map[string]interface{}, rM map[string]interface{}, cM map[string]interface{}, dontCare *bool, scale chan bool) {
	sum := 0
	for _, v := range dM["wT"].(map[string]interface{}) {
		sum += v.(int)
	}
	avgDMWT := sum / len(dM["wT"].(map[string]interface{}))
	sum = 0
	for _, v := range rM["wT"].(map[string]interface{}) {
		sum += v.(int)
	}
	avgRMWT := sum / len(rM["wT"].(map[string]interface{}))
	// TODO: add a tolerance
	// run wait time is greater than desired wait time
	if avgRMWT > avgDMWT {
		cM["instances"].(map[string]interface{})[*queryType] = cM["instances"].(map[string]interface{})[*queryType].(int) - 1
		scale <- false
	}
}

func (msc MasterSubscribeController) downscale(queryType *string, dM map[string]interface{}, rM map[string]interface{}, cM map[string]interface{}, dontCare *bool, scale chan bool) {
	sum := 0
	for _, v := range dM["wT"].(map[string]interface{}) {
		sum += v.(int)
	}
	avgDMWT := sum / len(dM["wT"].(map[string]interface{}))
	sum = 0
	for _, v := range rM["wT"].(map[string]interface{}) {
		sum += v.(int)
	}
	avgRMWT := sum / len(rM["wT"].(map[string]interface{}))
	// TODO: add a tolerance
	// run wait time is greater than desired wait time
	if avgRMWT > avgDMWT {
		cM["instances"].(map[string]interface{})[*queryType] = cM["instances"].(map[string]interface{})[*queryType].(int) - 1
		scale <- false
	}
}

func (mpc MasterPublishController) getUpdatedChunkSize(rm map[string]interface{}, cm map[string]interface{}) int {
	// TODO: more intelligence required
	return cm["chunk_size"].(int)
}

func (msc MasterSubscribeController) getUpdatedSleepTime(rm map[string]interface{}, cm map[string]interface{}) int {
	// TODO: more intelligence required
	return cm["sleep_time"].(int)
}

func (msc MasterSubscribeController) getUpdatedChunkSize(rm map[string]interface{}, cm map[string]interface{}) int {
	// TODO: more intelligence required
	return cm["chunk_size"].(int)
}

func (mpc MasterPublishController) getUpdatedSleepTime(rm map[string]interface{}, cm map[string]interface{}) int {
	// TODO: more intelligence required
	return cm["sleep_time"].(int)
}

func getConnection(host string, user string, pwd string, db string, port int) *sql.DB {
	// ping the database to see if it can connect
	conn, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", user, pwd, host, port, db))
	if err != nil {
		glog.Fatal(err) // err is actually an interface
	}
	return conn
}

func run() {

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
