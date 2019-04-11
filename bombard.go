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
	"database/sql"
	"flag"
	"fmt"
	"math"
	"os"
	"strconv"

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
	e     dbError
}

/*
QueryBatch defines the batch of queries to be executed
q is the query defined, sleep is the sleep time between queries in milliseconds, size is the number of queries in the batch
*/
type QueryBatch struct {
	q     Query
	sleep int
	size  int
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
