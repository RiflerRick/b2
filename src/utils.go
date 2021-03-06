package main

import (
	"database/sql"
	"fmt"
	"math"
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
	err = db.QueryRow(fmt.Sprintf("SELECT COUNT(1) FROM %s WHERE id >= %d and id < %d", table, startID, endID)).Scan(&count)
	return startID, count
}

func getColSubset(colSelect map[string]bool, allowIDSelection bool, indexedCols map[string]bool, allowMissingIndex bool) {
	numSelected := 0
	for k := range colSelect {
		if rand.Intn(2) == 1 {
			switch allowIDSelection {
			case false:
				switch allowMissingIndex {
				case false:
					if k != "id" && indexedCols[k] {
						colSelect[k] = true
						numSelected++
					}
				}
			case true:
				if k != "id" {
					colSelect[k] = true
					numSelected++
				}
			case true:
				switch allowMissingIndex {
				case false:
					if indexedCols[k] {
						colSelect[k] = true
						numSelected++
					}
				}
			case true:
				colSelect[k] = true
				numSelected++
			}
		} else {
			colSelect[k] = false
		}
	}
	// select everything except id field in case nothing got selected
	if numSelected < 1 {
		for k := range colSelect {
			switch allowIDSelection {
			case false:
				switch allowMissingIndex {
				case false:
					if k != "id" && indexedCols[k] {
						colSelect[k] = true
						numSelected++
					}
				}
			case true:
				if k != "id" {
					colSelect[k] = true
					numSelected++
				}
			case true:
				switch allowMissingIndex {
				case false:
					if indexedCols[k] {
						colSelect[k] = true
						numSelected++
					}
				}
			case true:
				colSelect[k] = true
				numSelected++
			}
		}
	}
}

func flushBus(bus chan *sql.Rows) {
	breakLoop := false
	for {
		select {
		case r := <-bus:
			glog.V(3).Infof("flusing and closing rows")
			r.Close()
		default:
			breakLoop = true
			break
		}
		if breakLoop {
			break
		}
	}
}

func allMetricPoll(pollTick int, dM DesiredMetadata, pubCM ControllerMetadata, subCM ControllerMetadata, c MetadataTimeSeries, r MetadataTimeSeries, u MetadataTimeSeries, d MetadataTimeSeries, stopSignal chan bool) {
	breakLoop := false
	for {
		select {
		case <-stopSignal:
			breakLoop = true
			break
		default:
			mpcCM := pollControllerMetrics(pubCM)
			mscCM := pollControllerMetrics(subCM)

			desiredMetadataMap := pollDesiredMetadata(dM)

			cpm, wT := pollMetadataMetrics(c)
			glog.V(0).Infof("DESIRED: create CPM: %d", desiredMetadataMap["create"].(int)*60)
			glog.V(0).Infof("RUN: create CPM: %d, create WT: %d", cpm*60, wT)
			cpm, wT = pollMetadataMetrics(r)
			glog.V(0).Infof("DESIRED: read CPM: %d", desiredMetadataMap["read"].(int)*60)
			glog.V(0).Infof("RUN: read CPM: %d, read WT: %d", cpm*60, wT)
			cpm, wT = pollMetadataMetrics(u)
			glog.V(0).Infof("DESIRED: update CPM: %d", desiredMetadataMap["update"].(int)*60)
			glog.V(0).Infof("RUN: update CPM: %d, update WT: %d", cpm*60, wT)
			cpm, wT = pollMetadataMetrics(d)
			glog.V(0).Infof("DESIRED: delete CPM: %d", desiredMetadataMap["delete"].(int)*60)
			glog.V(0).Infof("RUN: delete CPM: %d, delete WT: %d", cpm*60, wT)

			glog.V(0).Infof(" Publisher Instances: \t%v\n Subscriber Instances: \t%v\n", mpcCM, mscCM)
			time.Sleep(time.Duration(pollTick) * time.Millisecond)
		}
		if breakLoop {
			break
		}
	}

}

func pollDesiredMetadata(dM DesiredMetadata) map[string]interface{} {
	var createCPM interface{}
	var readCPM interface{}
	var updateCPM interface{}
	var deleteCPM interface{}

	typeOfData := "cpm"

	createQueryType := "create"
	readQueryType := "read"
	updateQueryType := "update"
	deleteQueryType := "delete"

	dM.read(&createQueryType, &typeOfData, &createCPM)
	dM.read(&readQueryType, &typeOfData, &readCPM)
	dM.read(&updateQueryType, &typeOfData, &updateCPM)
	dM.read(&deleteQueryType, &typeOfData, &deleteCPM)

	return map[string]interface{}{
		"create": createCPM,
		"read":   readCPM,
		"update": updateCPM,
		"delete": deleteCPM,
	}
}

func pollMetadataMetrics(timeSeries MetadataTimeSeries) (int, int) {
	t := timeSeries.readLatest()
	return t.cpm.(int), t.wT.(int)
}

func pollControllerMetrics(cM ControllerMetadata) map[string]interface{} {

	var createSubInstances interface{}
	var readSubInstances interface{}
	var updateSubInstances interface{}
	var deleteSubInstances interface{}

	var readPubInstances interface{}

	var typeOfQuery string
	var typeOfData string

	data := make(map[string]interface{})

	typeOfData = "instances"
	if cM.controllerType == "publish" {
		typeOfQuery = "read"
		cM.read(&typeOfQuery, &typeOfData, &readPubInstances)
		data["read"] = readPubInstances.(int)
	} else {
		typeOfQuery = "create"
		cM.read(&typeOfQuery, &typeOfData, &createSubInstances)
		typeOfQuery = "read"
		cM.read(&typeOfQuery, &typeOfData, &readSubInstances)
		typeOfQuery = "update"
		cM.read(&typeOfQuery, &typeOfData, &updateSubInstances)
		typeOfQuery = "delete"
		cM.read(&typeOfQuery, &typeOfData, &deleteSubInstances)

		data["create"] = createSubInstances.(int)
		data["read"] = readSubInstances.(int)
		data["update"] = updateSubInstances.(int)
		data["delete"] = deleteSubInstances.(int)
	}
	return data
}

func pushToTimeSeries(timeSeries MetadataTimeSeries, cpm int, wT int) error {
	var t timeSeriesPoint
	t.cpm = cpm
	t.wT = wT
	return timeSeries.write(t)
}

func computeMetrics(queryType string, timeSeriesTick int, timeSeries MetadataTimeSeries, qWT chan int, stopSignal chan bool) {
	totalQExecuted := 0
	totalWT := 0
	breakLoop := false
	cpm := 0
	wT := 0

	syncTimeSeriesControlCounter := 0

	startTime := time.Now()
	initialStartTime := startTime
	for {
		select {
		case <-stopSignal:
			breakLoop = true
			break
		default:
			timeElapsed := (time.Now()).Sub(startTime)
			totalTimeElapsed := (time.Now()).Sub(initialStartTime)
			if int(int(totalTimeElapsed.Seconds()*1000)/timeSeriesTick) > syncTimeSeriesControlCounter {
				glog.V(3).Infof("syncing CPM and WT to timeSeries")
				err := pushToTimeSeries(timeSeries, cpm/totalQExecuted, wT/totalQExecuted)
				if err != nil {
					glog.V(3).Infof("Could not push to timeSeries")
					return
				}
				syncTimeSeriesControlCounter++
				totalQExecuted = 0
				totalWT = 0
				cpm = 0
				wT = 0
				startTime = time.Now()
			}
			totalWT += <-qWT
			totalQExecuted++

			timeElapsedSeconds := timeElapsed.Seconds()
			cpm += int(math.Round(float64(totalQExecuted) / timeElapsedSeconds))
			wT += int(math.Round(float64(totalWT / totalQExecuted)))
		}
		if breakLoop {
			break
		}
	}
}

func getQuery(queryType *string, tableName *string, writeChunkSize int, colData map[string]interface{}, indexedCols map[string]bool, allowMissingIndex map[string]bool) (string, []interface{}) {
	/*
		returns the normalized query and the data in a slice
	*/

	var query string
	colSelect := make(map[string]bool)
	var data []interface{}
	for k := range colData {
		colSelect[k] = false
	}
	for true {
		if *queryType == "read" {
			getColSubset(colSelect, false, indexedCols, allowMissingIndex["read"])
			baseQuery := fmt.Sprintf("SELECT * FROM %s WHERE ", *tableName)
			for k, v := range colData {
				if colSelect[k] {
					baseQuery += fmt.Sprintf("%s = ? and ", k)
					data = append(data, v)
				}
			}
			baseQuery = strings.TrimSuffix(baseQuery, " and ")
			query = baseQuery
			break
		} else if *queryType == "create" {
			baseQuery := fmt.Sprintf("INSERT INTO %s", *tableName)
			var columnName string
			var columnData string
			for k, v := range colData {
				if k == "id" {
					continue
				}
				columnName += fmt.Sprintf("%s, ", k)
				columnData += fmt.Sprintf("?, ")
				data = append(data, v)
			}
			columnName = strings.TrimSuffix(columnName, ", ")
			columnData = strings.TrimSuffix(columnData, ", ")
			baseQuery += fmt.Sprintf(" (%s) VALUES(%s)", columnName, columnData)
			query = baseQuery
			break
		} else if *queryType == "update" {
			baseQuery := fmt.Sprintf("UPDATE %s SET ", *tableName)
			getColSubset(colSelect, false, indexedCols, allowMissingIndex["update"])
			for k, v := range colData {
				if colSelect[k] {
					baseQuery += fmt.Sprintf("%s = ?,", k)
					data = append(data, v)
				}
			}
			baseQuery = strings.TrimSuffix(baseQuery, ",") + " WHERE "
			getColSubset(colSelect, false, indexedCols, allowMissingIndex["update"])
			for k, v := range colData {
				if colSelect[k] {
					baseQuery += fmt.Sprintf("%s = ? and ", k)
					data = append(data, v)
				}
			}
			baseQuery = strings.TrimSuffix(baseQuery, " and ")
			query = baseQuery
			break
		} else {
			baseQuery := fmt.Sprintf("DELETE FROM %s ", *tableName)
			getColSubset(colSelect, false, indexedCols, allowMissingIndex["delete"])
			for k, v := range colData {
				if colSelect[k] {
					baseQuery += fmt.Sprintf("%s = ? and ", k)
					data = append(data, v)
				}
			}
			baseQuery = strings.TrimSuffix(baseQuery, " and ")
			query = baseQuery
			break
		}
	}
	return query, data
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
			tx.execute(baseQuery, colData...)
			tx.commit()
			startTrx = true
		}
	}
	if rowsFetched%insertCommitsAfter != 0 {
		baseQuery = strings.TrimSuffix(baseQuery, ",")
		tx.execute(baseQuery, colData...) // to handle the last bit
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
