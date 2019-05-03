package main

import (
	"database/sql"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/golang/glog"
)

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

func getRunChunk(db *sql.DB, table string, runN int, prepN int) (int, int) {
	var startID int
	var endID int
	var count int
	err := db.QueryRow(fmt.Sprintf("SELECT id FROM %s ORDER BY ID DESC LIMIT 1 OFFSET %d", table, runN)).Scan(&startID)
	if err != nil {
		glog.Fatal(err)
	}
	err = db.QueryRow(fmt.Sprintf("SELECT id FROM %s ORDER BY ID DESC LIMIT 1 OFFSET %d", table, prepN)).Scan(&endID)
	if err != nil {
		glog.Fatal(err)
	}
	err = db.QueryRow(fmt.Sprintf("SELECT COUNT(1) FROM %s WHERE id >= %d and i < %d", table, startID, endID)).Scan(&count)
	return startID, count
}

/*
	function to publish data to the bus after reading from the source db
	to be called as a go routine. publishes data to the bus channel to be consumed by bombarding routines
*/
func publishToBus(db *sql.DB, table *string, startID *int, count *int, readChunkSize *int, sleepTime *int, bus chan *sql.Rows, stopSignal chan bool) {

	source := rand.NewSource(time.Now().UnixNano())
	r := rand.New(source)
	// timeNow := time.Now()
	// if int(math.Round(timeNow.Sub(*startTime).Minutes())) > timeToRun {
	// 	break
	// }
	for {
		select {
		case <-stopSignal:
			break
		default:
			offset := r.Intn(*count)
			rows, err := db.Query(fmt.Sprintf("SELECT * FROM %s WHERE id >= %d LIMIT %d OFFSET %d", table, startID, *readChunkSize, offset))
			if err != nil {
				glog.Info(err)
				return
			}
			bus <- rows
			time.Sleep(time.Duration(*sleepTime) * time.Millisecond)
		}
	}
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

func getQuery(queryType *string, tableName *string, writeChunkSize *int, colData map[string]interface{}, indexedCols map[string]bool, allowMissingIndex map[string]bool) string {

	var query string
	var colSelect map[string]bool
	for k := range colData {
		colSelect[k] = false
	}
	for true {
		if *queryType == "read" {
			getSubset(colSelect)
			baseQuery := fmt.Sprintf("SELECT * FROM %s WHERE ", *tableName)
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
			query = baseQuery
			break
		} else if *queryType == "create" {
			baseQuery := fmt.Sprintf("INSERT INTO %s VALUES (", *tableName)
			for _, v := range colData {
				baseQuery += fmt.Sprintf("%s, ", v)
			}
			baseQuery = strings.TrimSuffix(baseQuery, ",")
			baseQuery += fmt.Sprintf(")")
			query = baseQuery
			break
		} else if *queryType == "update" {
			baseQuery := fmt.Sprintf("UPDATE %s SET ", *tableName)
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
			query = baseQuery
			break
		} else {
			baseQuery := fmt.Sprintf("DELETE FROM %s ", *tableName)
			getSubset(colSelect)
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
			query = baseQuery
			break
		}
	}
	return query
}

/*
subscribe to bus for hitting the db. sleep decides how much to sleep in between queries
queryTypeCPM: map storing the CPM values for each query. The type of query to be fired will be chosen by this CPM
*/
func bombard(queryType *string, tableName *string, db *sql.DB, sleepTime *int, writeChunkSize *int, bus chan *sql.Rows, indexedCols map[string]bool, allowMissingIndex map[string]bool, qWT chan int, busEmpty chan string, stopSignal chan bool) {
	var r *sql.Rows
	var q Query
	for {
		select {
		case <-stopSignal:
			break
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
				query := getQuery(queryType, tableName, writeChunkSize, colData, indexedCols, allowMissingIndex)
				if *queryType == "select" {
					q.query = query
					q.executeRead(db)
					qWT <- q.wt
					time.Sleep(time.Millisecond * time.Duration(*sleepTime))
				} else {
					q.query = query
					q.executeWrite(db)
					qWT <- q.wt
					time.Sleep(time.Millisecond * time.Duration(*sleepTime))
				}
			}
		default:
			// bus should never be empty
			busEmpty <- *queryType
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

func contains(slice []string, item string) bool {
	set := make(map[string]struct{}, len(slice))
	for _, s := range slice {
		set[s] = struct{}{}
	}

	_, ok := set[item]
	return ok
}
