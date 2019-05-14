package main

import (
	"database/sql"
	"math"
	"time"

	"github.com/golang/glog"
)

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

func (q *Query) executeRead(db *sql.DB, data ...interface{}) *sql.Rows {
	/*
		for executing normalized queries
	*/
	st := time.Now()
	rows, err := db.Query(q.query, data...)
	et := time.Now()
	q.wt = int(math.Round(et.Sub(st).Seconds() * 1000 * 1000))
	if err != nil {
		glog.Fatal(err)
	}
	// as long as there is an open result set(represented by rows), the underlying connection is busy and can't be used for any other query
	// That means it is not available in the connection pool. If you iterate over all the rows with rows.Next(), eventually you'll read the last row and rows.Next()
	// will encounter an internal EOF call and call rows.Close(). But if for some reason rows.Close() is not called and we exit the function, not defering rows.Close()
	// can become a potential source of memory leak in that case. In case of an error however rows.Close() is called internally
	return rows
}

func (q *Query) executeReadRow(db *sql.DB, data ...interface{}) *sql.Row {
	st := time.Now()
	row := db.QueryRow(q.query, data...)
	et := time.Now()
	q.wt = int(math.Round(et.Sub(st).Seconds() * 1000 * 1000))
	return row
}

func (q *Query) executeReadAsync(db *sql.DB, rowChan chan *sql.Rows, data ...interface{}) {
	st := time.Now()
	rows, err := db.Query(q.query, data...)
	et := time.Now()
	q.wt = int(math.Round(et.Sub(st).Seconds() * 1000 * 1000))
	if err != nil {
		glog.Info(err)
		return
	}
	rowChan <- rows
}

func (q *Query) executeReadRowAsync(db *sql.DB, rowChan chan *sql.Row, data ...interface{}) {
	st := time.Now()
	row := db.QueryRow(q.query, data...)
	et := time.Now()
	q.wt = int(math.Round(et.Sub(st).Seconds() * 1000 * 1000))
	rowChan <- row
}

func (q *Query) executeWrite(db *sql.DB, data ...interface{}) {
	st := time.Now()
	_, err := db.Exec(q.query, data...)
	et := time.Now()
	if err != nil {
		glog.Fatal(err)
	}
	q.wt = int(math.Round(et.Sub(st).Seconds() * 1000 * 1000))
}

func (t *Transaction) commit() {
	st := time.Now()
	err := t.transaction.Commit()
	if err != nil {
		glog.Fatal(err)
	}
	et := time.Now()
	t.wt = int(math.Round(et.Sub(st).Seconds() * 1000 * 1000))
}

func (t *Transaction) execute(query string, data ...interface{}) {
	_, err := t.transaction.Exec(query, data...)
	if err != nil {
		glog.Fatal(err)
	}
}
