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
	"reflect"
	"strconv"
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
dM: desiredMetadata consisting of CPM, wT //this never changes
rM: runMetadata consisting of CPM, wT (actual)
cM: controllerMetadata consisting of instances_running, sleepTime and chunkSize
*/

/*
Metadata that has to be accessed by multiple entities
*/
type Metadata interface {
	read(queryType *string, typeOfData *string, data *interface{})
	write(queryType *string, typeOfData *string, data interface{})
}

// mutex cannot obviously be part of the resource being shared
// TODO: instead of declaring variables of type sync.RWMutex, there must be a better way to do this

var createDMCPMMutex sync.RWMutex
var readDMCPMMutex sync.RWMutex
var updateDMCPMMutex sync.RWMutex
var deleteDMCPMMutex sync.RWMutex

var createDMWTMutex sync.RWMutex
var readDMWTMutex sync.RWMutex
var updateDMWTMutex sync.RWMutex
var deleteDMWTMutex sync.RWMutex

var createRMCPMMutex sync.RWMutex
var readRMCPMMutex sync.RWMutex
var updateRMCPMMutex sync.RWMutex
var deleteRMCPMMutex sync.RWMutex

var createRMWTMutex sync.RWMutex
var readRMWTMutex sync.RWMutex
var updateRMWTMutex sync.RWMutex
var deleteRMWTMutex sync.RWMutex

var createCMInstancesMutex sync.RWMutex
var readCMInstancesMutex sync.RWMutex
var updateCMInstancesMutex sync.RWMutex
var deleteCMInstancesMutex sync.RWMutex

var createCMSleepMutex sync.RWMutex
var readCMSleepMutex sync.RWMutex
var updateCMSleepMutex sync.RWMutex
var deleteCMSleepMutex sync.RWMutex

var createCMChunkMutex sync.RWMutex
var readCMChunkMutex sync.RWMutex
var updateCMChunkMutex sync.RWMutex
var deleteCMChunkMutex sync.RWMutex

var dmCPMMutex map[string]*sync.RWMutex
var dmWTMutex map[string]*sync.RWMutex

var rmCPMMutex map[string]*sync.RWMutex
var rmWTMutex map[string]*sync.RWMutex

var cmInstancesMutex map[string]*sync.RWMutex
var cmSleepTimeMutex map[string]*sync.RWMutex
var cmChunkSizeMutex map[string]*sync.RWMutex

/*
DesiredMetadata will never change. contains calls per minute and wait time
For each type, the keys of the map will be the querytypes
*/
type DesiredMetadata struct {
	cpm map[string]interface{}
	wT  map[string]interface{}
}

/*
RunMetadata will be updated during run phase. contains calls per minute and wait time
For each type, the keys of the map will be the querytypes
*/
type RunMetadata struct {
	cpm map[string]interface{}
	wT  map[string]interface{}
}

/*
ControllerMetadata is the metadata maintained by MasterPublishController and MasterSubscribeController
For each type, the keys of the map will be the querytypes
*/
type ControllerMetadata struct {
	instances map[string]interface{}
	chunkSize map[string]interface{}
	sleepTime map[string]interface{}
}

/*
MasterPublishController for controlling all publishers
*/
type MasterPublishController struct {
	cM        ControllerMetadata
	tableName *string
	db        *sql.DB
}

/*
MasterSubscribeController for controlling all subscribers
*/
type MasterSubscribeController struct {
	cM        ControllerMetadata
	tableName *string
	db        *sql.DB
}

func (mpc MasterPublishController) upscale(queryType *string, dM DesiredMetadata, rM RunMetadata, dontCare *bool) bool {
	if *dontCare {
		return true
	}
	return false
}

func (msc MasterSubscribeController) upscale(queryType *string, dM DesiredMetadata, rM RunMetadata, dontCare *bool) bool {
	typeOfData := "cpm"
	var rCPM interface{}
	var dCPM interface{}
	rM.read(queryType, &typeOfData, &rCPM)
	dM.read(queryType, &typeOfData, &dCPM)
	if rCPM.(int) < dCPM.(int) {
		return true
	}
	return false
}

func (mpc MasterPublishController) downscale(queryType *string, dM DesiredMetadata, rM RunMetadata, dontCare *bool) bool {
	sum := 0
	var createVal interface{}
	var readVal interface{}
	var updateVal interface{}
	var deleteVal interface{}
	wT := "wT"
	possibleQueryTypes := []string{"create", "read", "update", "delete"}
	dM.read(&possibleQueryTypes[0], &wT, &createVal)
	dM.read(&possibleQueryTypes[1], &wT, &readVal)
	dM.read(&possibleQueryTypes[2], &wT, &updateVal)
	dM.read(&possibleQueryTypes[3], &wT, &deleteVal)
	sum = createVal.(int) + readVal.(int) + updateVal.(int) + deleteVal.(int)
	avgDMWT := sum / 4
	sum = 0
	rM.read(&possibleQueryTypes[0], &wT, &createVal)
	rM.read(&possibleQueryTypes[1], &wT, &readVal)
	rM.read(&possibleQueryTypes[2], &wT, &updateVal)
	rM.read(&possibleQueryTypes[3], &wT, &deleteVal)
	sum = createVal.(int) + readVal.(int) + updateVal.(int) + deleteVal.(int)
	avgRMWT := sum / 4
	// TODO: add a tolerance
	// run wait time is greater than desired wait time
	if avgRMWT > avgDMWT {
		return true
	}
	return false
}

func (msc MasterSubscribeController) downscale(queryType *string, dM DesiredMetadata, rM RunMetadata, dontCare *bool) bool {
	var avgDMWT interface{}
	wT := "wT"
	dM.read(queryType, &wT, &avgDMWT)
	var avgRMWT interface{}
	rM.read(queryType, &wT, &avgRMWT)
	dmWT, ok := avgDMWT.(int)
	if !ok {
		// its possible the number is infinite which is of type float64
		if reflect.TypeOf(avgDMWT).String() == "float64" && math.IsInf(avgDMWT.(float64), 1) {
			return false
		}
	}
	rmWT, ok := avgRMWT.(int)
	if !ok {
		// its possible the number is infinite which is of type float64
		if reflect.TypeOf(avgDMWT).String() == "float64" && math.IsInf(avgRMWT.(float64), 1) {
			return true
		}
	}
	// TODO: add a tolerance
	// run wait time is greater than desired wait time
	if rmWT > dmWT {
		return true
	}
	return false
}

func computeMetrics(queryType string, rM RunMetadata, qWT chan int, stopSignal chan bool) {
	metricVisibilityWindow := 500 // in milliseconds
	totalQExecuted := 0
	totalWT := 0
	startTime := time.Now()
	for {
		select {
		case <-stopSignal:
			break
		default:
			totalWT += <-qWT
			timeElapsed := (time.Now()).Sub(startTime)
			timeElapsedMin := timeElapsed.Minutes()
			timeElapsedMil := int(timeElapsed.Seconds() * 1000)
			var rMCPM interface{}
			var rMWT interface{}
			cpmType := "cpm"
			wTType := "wT"
			rM.read(&queryType, &cpmType, &rMCPM)
			rM.read(&queryType, &wTType, &rMCPM)

			if timeElapsedMil%metricVisibilityWindow == 0 {
				glog.V(1).Infof("queryType: %s; CPM: %d; wT: %d ms", queryType, rMCPM.(int), rMWT.(int))
			}
			totalQExecuted++
			// fmt.Printf("cpm for queryType: %s is %d", queryType, int(math.Round(float64(totalQExecuted)/timeElapsedMin)))
			// fmt.Printf("waitTime for queryType: %s is %d", queryType, int(math.Round(float64(totalWT/totalQExecuted))))
			rM.write(&queryType, &cpmType, int(math.Round(float64(totalQExecuted)/timeElapsedMin)))
			rM.write(&queryType, &wTType, int(math.Round(float64(totalWT/totalQExecuted))))
		}
	}
}

func (msc MasterSubscribeController) run(queryType string, dM DesiredMetadata, rM RunMetadata, timeToRun int, indexedColumns map[string]bool, allowMissingIndex map[string]bool, busEmpty chan string, bus chan *sql.Rows, qWT chan int, wg *sync.WaitGroup) {
	defer wg.Done()

	subscribeDontCare := false
	var canUpscale bool
	var canDownscale bool
	subscriberStopSignal := make(chan bool)

	stopMetricCompute := make(chan bool)
	glog.V(1).Infof("Spawning computeMetric routine for queryType: %s", queryType)
	go computeMetrics(queryType, rM, qWT, stopMetricCompute)
	startTime := time.Now()

	var currentInstances interface{}
	typeOfData := "instances"
	for true {
		if (time.Now()).Sub(startTime).Minutes() > float64(timeToRun) {
			break
		}
		canDownscale = msc.downscale(&queryType, dM, rM, &subscribeDontCare)
		canUpscale = msc.upscale(&queryType, dM, rM, &subscribeDontCare)

		if canDownscale {
			glog.V(1).Info("downscaling subscriber instances by one")
			msc.cM.read(&queryType, &typeOfData, &currentInstances)
			currentInstances = currentInstances.(int) - 1
			msc.cM.write(&queryType, &typeOfData, &currentInstances)
			subscriberStopSignal <- true
		} else if canUpscale {
			glog.V(1).Info("upscaling subscriber instances by one")
			msc.cM.read(&queryType, &typeOfData, &currentInstances)
			currentInstances = currentInstances.(int) + 1
			msc.cM.write(&queryType, &typeOfData, &currentInstances)
			go msc.bombard(&queryType, bus, indexedColumns, allowMissingIndex, qWT, busEmpty, subscriberStopSignal)
		}
	}
	glog.V(1).Info("Tearing down all subscriber instances")
	msc.cM.read(&queryType, &typeOfData, &currentInstances)
	for i := 0; i < currentInstances.(int); i++ {
		subscriberStopSignal <- true
	}
	glog.V(1).Info("Tearing down all `computeMetrics` instances")
	for i := 0; i < 4; i++ {
		stopMetricCompute <- true
	}
}

func (mpc MasterPublishController) run(queryType string, dM DesiredMetadata, rM RunMetadata, timeToRun int, bus chan *sql.Rows, busEmpty chan string, wg *sync.WaitGroup, startID int, runChunk int) {
	defer wg.Done()

	publishDontCare := false
	var canUpscale bool
	var canDownscale bool
	publisherStopSignal := make(chan bool)
	startTime := time.Now()
	var currentInstances interface{}
	typeOfData := "instances"
	for true {
		if (time.Now()).Sub(startTime).Minutes() > float64(timeToRun) {
			break
		}
		for {
			select {
			case <-busEmpty:
				glog.V(1).Info("Bus found to be empty")
				publishDontCare = true
				break
			default:
				break
			}
		}
		canDownscale = mpc.downscale(&queryType, dM, rM, &publishDontCare)
		canUpscale = mpc.upscale(&queryType, dM, rM, &publishDontCare)
		publishDontCare = false

		if canDownscale {
			glog.V(1).Info("downscaling publisher instances by 1")
			mpc.cM.read(&queryType, &typeOfData, &currentInstances)
			currentInstances = currentInstances.(int) - 1
			mpc.cM.write(&queryType, &typeOfData, &currentInstances)
			publisherStopSignal <- true
		} else if canUpscale {
			glog.V(1).Info("upscaling publisher instances by 1")
			mpc.cM.read(&queryType, &typeOfData, &currentInstances)
			currentInstances = currentInstances.(int) + 1
			mpc.cM.write(&queryType, &typeOfData, &currentInstances)
			go mpc.publishToBus(&startID, &runChunk, bus, publisherStopSignal)
		}
	}
	glog.V(1).Info("Tearing down all publisher instances")
	mpc.cM.read(&queryType, &typeOfData, &currentInstances)
	for i := 0; i < currentInstances.(int); i++ {
		publisherStopSignal <- true
	}
}

func (mpc MasterPublishController) getUpdatedChunkSize(queryType *string, rm RunMetadata, cm ControllerMetadata) int {
	// TODO: more intelligence required
	var chunkSize interface{}
	typeOfData := "chunk_size"
	cm.read(queryType, &typeOfData, &chunkSize)
	return chunkSize.(int)
}

func (msc MasterSubscribeController) getUpdatedSleepTime(queryType *string, rm RunMetadata, cm ControllerMetadata) int {
	// TODO: more intelligence required
	var sleepTime interface{}
	typeOfData := "sleep_time"
	cm.read(queryType, &typeOfData, &sleepTime)
	return sleepTime.(int)
}

func (msc MasterSubscribeController) getUpdatedChunkSize(queryType *string, rm RunMetadata, cm ControllerMetadata) int {
	// TODO: more intelligence required
	var chunkSize interface{}
	typeOfData := "chunk_size"
	cm.read(queryType, &typeOfData, &chunkSize)
	return chunkSize.(int)
}

func (mpc MasterPublishController) getUpdatedSleepTime(queryType *string, rm RunMetadata, cm ControllerMetadata) int {
	// TODO: more intelligence required
	var sleepTime interface{}
	typeOfData := "sleep_time"
	cm.read(queryType, &typeOfData, &sleepTime)
	return sleepTime.(int)
}

/*
dM: desiredMetadata consisting of CPM, wT //this never changes
rM: runMetadata consisting of CPM, wT (actual)
cM: controllerMetadata consisting of instances_running, sleepTime and chunkSize
*/
func run(publishSleepTime int, subscribeSleepTime int, publishChunkSize int, subscribeChunkSize int, db *sql.DB, expDB *sql.DB, tableSchema string, tableName string, allowMissingIndex map[string]bool, prepN int, runN int, time int, createCPM int, readCPM int, updateCPM int, deleteCPM int) {
	cmInstancesMutex = map[string]*sync.RWMutex{
		"create": &createCMInstancesMutex,
		"read":   &readCMInstancesMutex,
		"update": &updateCMInstancesMutex,
		"delete": &deleteCMInstancesMutex,
	}

	cmChunkSizeMutex = map[string]*sync.RWMutex{
		"create": &createCMChunkMutex,
		"read":   &readCMChunkMutex,
		"update": &updateCMChunkMutex,
		"delete": &deleteCMChunkMutex,
	}

	cmSleepTimeMutex = map[string]*sync.RWMutex{
		"create": &createCMSleepMutex,
		"read":   &readCMSleepMutex,
		"update": &updateCMSleepMutex,
		"delete": &deleteCMSleepMutex,
	}

	dmCPMMutex = map[string]*sync.RWMutex{
		"create": &createDMCPMMutex,
		"read":   &readDMCPMMutex,
		"update": &updateDMCPMMutex,
		"delete": &deleteDMCPMMutex,
	}

	dmWTMutex = map[string]*sync.RWMutex{
		"create": &createDMWTMutex,
		"read":   &readDMWTMutex,
		"update": &updateDMWTMutex,
		"delete": &deleteDMWTMutex,
	}

	rmCPMMutex = map[string]*sync.RWMutex{
		"create": &createRMCPMMutex,
		"read":   &readRMCPMMutex,
		"update": &updateRMCPMMutex,
		"delete": &deleteRMCPMMutex,
	}

	rmWTMutex = map[string]*sync.RWMutex{
		"create": &createRMWTMutex,
		"read":   &readRMWTMutex,
		"update": &updateRMWTMutex,
		"delete": &deleteRMWTMutex,
	}

	var mpc MasterPublishController
	var msc MasterSubscribeController
	mpc.cM.instances = map[string]interface{}{
		"create": 0,
		"read":   0,
		"update": 0,
		"delete": 0,
	}
	mpc.cM.sleepTime = map[string]interface{}{
		"create": publishSleepTime,
		"read":   publishSleepTime,
		"update": publishSleepTime,
		"delete": publishSleepTime,
	}
	mpc.cM.chunkSize = map[string]interface{}{
		"create": publishChunkSize,
		"read":   publishChunkSize,
		"update": publishChunkSize,
		"delete": publishChunkSize,
	}
	msc.cM.instances = map[string]interface{}{
		"create": 0,
		"read":   0,
		"update": 0,
		"delete": 0,
	}
	msc.cM.sleepTime = map[string]interface{}{
		"create": subscribeSleepTime,
		"read":   subscribeSleepTime,
		"update": subscribeSleepTime,
		"delete": subscribeSleepTime,
	}
	msc.cM.chunkSize = map[string]interface{}{
		"create": subscribeChunkSize,
		"read":   subscribeChunkSize,
		"update": subscribeChunkSize,
		"delete": subscribeChunkSize,
	}

	// get indexed columns
	indexedColumnsMap := make(map[string]bool)
	var indexedColumns []string
	var indexedColumnName string
	var columnName string
	indices, err := db.Query("select column_name from information_schema.statistics where table_schema=? and table_name=? and index_name != ?", tableSchema, tableName, "PRIMARY")
	if err != nil {
		glog.Fatal(err)
	}
	defer indices.Close()
	for indices.Next() {
		err := indices.Scan(&indexedColumnName)
		if err != nil {
			glog.Fatal(err)
		}
		indexedColumns = append(indexedColumns, indexedColumnName)
	}
	columns, err := db.Query("select column_name from information_schema.columns where table_schema=? and table_name=?", tableSchema, tableName)
	if err != nil {
		glog.Fatal(err)
	}
	defer columns.Close()
	for columns.Next() {
		err := columns.Scan(&columnName)
		if err != nil {
			glog.Fatal(err)
		}
		if contains(indexedColumns, columnName) {
			indexedColumnsMap[columnName] = true
		} else {
			indexedColumnsMap[columnName] = false
		}
	}
	var hasConstraints int
	err = db.QueryRow("select count(1) from information_schema.table_constraints where table_schema=? and table_name=? and constraint_type=?", tableSchema, tableName, "UNIQUE").Scan(&hasConstraints)
	if err != nil {
		glog.Fatal(err)
	}
	var startID int
	var count int
	if hasConstraints > 0 {
		startID, count = getRunChunk(db, tableName, runN, prepN)
	} else {
		startID, count = getRunChunk(db, tableName, prepN, 0)
	}
	var desiredMetadata DesiredMetadata
	var runMetadata RunMetadata
	desiredMetadata.cpm = map[string]interface{}{
		"create": createCPM,
		"read":   readCPM,
		"update": updateCPM,
		"delete": deleteCPM,
	}
	desiredMetadata.wT = make(map[string]interface{})
	if createCPM == 0 {
		desiredMetadata.wT["create"] = math.Inf(1)
	} else {
		desiredMetadata.wT["create"] = (60 / createCPM) * 1000
	}

	if readCPM == 0 {
		desiredMetadata.wT["read"] = math.Inf(1)
	} else {
		desiredMetadata.wT["read"] = (60 / readCPM) * 1000
	}

	if updateCPM == 0 {
		desiredMetadata.wT["update"] = math.Inf(1)
	} else {
		desiredMetadata.wT["update"] = (60 / updateCPM) * 1000
	}

	if deleteCPM == 0 {
		desiredMetadata.wT["delete"] = math.Inf(1)
	} else {
		desiredMetadata.wT["delete"] = (60 / deleteCPM) * 1000
	}

	runMetadata.cpm = map[string]interface{}{
		"create": 0,
		"read":   0,
		"update": 0,
		"delete": 0,
	}
	runMetadata.wT = map[string]interface{}{
		"create": 0,
		"read":   0,
		"update": 0,
		"delete": 0,
	}
	glog.V(1).Infof("Starting publishers")
	var wg sync.WaitGroup
	busEmpty := make(chan string)
	bus := make(chan *sql.Rows)
	wg.Add(5)
	go mpc.run("select", desiredMetadata, runMetadata, time, bus, busEmpty, &wg, startID, count)

	createQWT := make(chan int)
	readQWT := make(chan int)
	updateQWT := make(chan int)
	deleteQWT := make(chan int)

	glog.V(1).Info("Starting subscribers")
	go msc.run("create", desiredMetadata, runMetadata, time, indexedColumnsMap, allowMissingIndex, busEmpty, bus, createQWT, &wg)
	go msc.run("read", desiredMetadata, runMetadata, time, indexedColumnsMap, allowMissingIndex, busEmpty, bus, readQWT, &wg)
	go msc.run("update", desiredMetadata, runMetadata, time, indexedColumnsMap, allowMissingIndex, busEmpty, bus, updateQWT, &wg)
	go msc.run("delete", desiredMetadata, runMetadata, time, indexedColumnsMap, allowMissingIndex, busEmpty, bus, deleteQWT, &wg)
	glog.V(1).Info("Waiting for MasterPublishController and MasterSubscribeController to finish")
	wg.Wait()

}

func prepare(db *sql.DB, expDb *sql.DB, table string, prepN int, prepareChunkSize int, insertCommitsAfter int) {
	/*
		prepare creates a new temporary table using the same schema as the specified table and copies `pr` amount of data to it
	*/
	glog.V(2).Infof("prepN has been set as %d", prepN)
	chunkCopyDataTempTable(db, expDb, table, prepN, prepareChunkSize, insertCommitsAfter)
}

func getConnection(host string, user string, pwd string, db string, port int) *sql.DB {
	// ping the database to see if it can connect
	conn, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", user, pwd, host, port, db))
	if err != nil {
		glog.Fatal(err) // err is actually an interface
	}
	return conn
}

func main() {

	approxTableSize, _ := strconv.ParseInt(os.Getenv("APPROX_TABLE_SIZE"), 10, 0)
	insertCommitsAfter, _ := strconv.ParseInt(os.Getenv("INSERT_COMMITS_AFTER"), 10, 0)
	prepPhaseChunkSize, _ := strconv.ParseInt(os.Getenv("PREP_PHASE_CHUNK_SIZE"), 10, 0)
	runPhasePublishChunkSize, _ := strconv.ParseInt(os.Getenv("RUN_PHASE_PUBLISH_CHUNK_SIZE"), 10, 0)
	// runPhaseSubscribeChunkSize, _ := strconv.ParseInt(os.Getenv("RUN_PHASE_SUBSCRIBE_CHUNK_SIZE"), 10, 0)
	runPhasePublishSleepTime, _ := strconv.ParseInt(os.Getenv("RUN_PHASE_PUBLISH_SLEEP_TIME"), 10, 0)
	runPhaseSubscribeSleepTime, _ := strconv.ParseInt(os.Getenv("RUN_PHASE_SUBSCRIBE_SLEEP_TIME"), 10, 0)

	tempTablePrepSizeRatio, _ := strconv.ParseFloat(os.Getenv("TEMP_TABLE_PREP_SIZE_RATIO"), 32) // the ratio of the temp table size to the actual table size, this amount of data is copied
	// to the temporary table from the new table
	tempTableRunSizeRatio, _ := strconv.ParseFloat(os.Getenv("TEMP_TABLE_RUN_SIZE_RATIO"), 32)

	if insertCommitsAfter == 0 {
		defVal := 1000
		glog.V(0).Infof("INSERT_COMMITS_AFTER has not been set, defaulting to %d", defVal)
		insertCommitsAfter = int64(defVal)
	}
	if tempTablePrepSizeRatio == 0.00 {
		defVal := 0.66
		glog.V(0).Infof("TEMP_TABLE_PREP_SIZE_RATIO has not been set, defaulting to %0.2f", defVal)
		tempTablePrepSizeRatio = defVal
	}
	if prepPhaseChunkSize == 0 {
		defVal := 10000
		glog.V(0).Infof("PREP_PHASE_CHUNK_SIZE has not been set, defaulting to %d", defVal)
		prepPhaseChunkSize = int64(defVal)
	}
	if tempTableRunSizeRatio == 0 {
		defVal := 0.25
		glog.V(0).Infof("TEMP_TABLE_RUN_SIZE_RATIO has not been set, defaulting to %0.2f", defVal)
		tempTableRunSizeRatio = defVal
	}
	if runPhasePublishChunkSize == 0 {
		defVal := 10000
		glog.V(0).Infof("RUN_PHASE_PUBLISH_CHUNK_SIZE has not been set, defaulting to %d", defVal)
		runPhasePublishChunkSize = int64(defVal)
	}
	if runPhasePublishSleepTime == 0 {
		defVal := 200
		glog.V(0).Infof("RUN_PHASE_PUBLISH_SLEEP_TIME has not been set, defaulting to %d", defVal)
		runPhasePublishSleepTime = int64(defVal)
	}
	if runPhaseSubscribeSleepTime == 0 {
		defVal := 100
		glog.V(0).Infof("RUN_PHASE_SUBSCRIBE_SLEEP_TIME has not been set, defaulting to %d", defVal)
		runPhaseSubscribeSleepTime = int64(defVal)
	}

	// TODO: this feature has not been added yet, once added uncomment this part
	// if runPhaseSubscribeChunkSize == 0 {
	// 	defVal := 10000
	// 	glog.V(0).Infof("RUN_PHASE_SUBSCRIBE_CHUNK_SIZE has not been set, defaulting to %d", defVal)
	// 	runPhaseSubscribeChunkSize = int64(defVal)
	// }
	host := flag.String("host", "localhost", "hostname of the database")
	expHost := flag.String("experiment-host", "localhost", "In case the benchmarking is being done in a separate host from where the actual table is present in, use this flag")
	user := flag.String("username", "root", "username")
	pwd := flag.String("password", "toor", "password")
	port := flag.Int("port", 3306, "port")
	db := flag.String("database", "ilapahsi", "database to execute on")
	askPass := flag.Bool("ask-pass", false, "Ask Pass")
	table := flag.String("tablename", "ilapahsi", "tablename")
	prepPhase := flag.Bool("prepare", false, "if prepare")
	runPhase := flag.Bool("run", false, "if run")
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
	if *prepPhase {
		glog.V(0).Info("Running 'prep' phase!!!")
		conn := getConnection(*host, *user, *pwd, *db, *port)
		expConn := getConnection(*expHost, *user, *pwd, *db, *port)
		glog.V(0).Infof("Starting prepare phase for table: %s", *table)

		var prepN int
		if approxTableSize == 0 {
			glog.V(0).Info("APPROX_TABLE_SIZE has not been provided. Falling back to count rows from table")
			glog.V(0).Infof("Executing: SELECT COUNT(1) FROM %s", *table)
			var count int
			err := conn.QueryRow(fmt.Sprintf("SELECT COUNT(1) FROM %s", *table)).Scan(&count)
			if err != nil {
				glog.Fatal(err)
			}
			prepN = int(math.Round(tempTablePrepSizeRatio * float64(count)))
		} else {
			prepN = int(math.Round(tempTablePrepSizeRatio * float64(approxTableSize)))
		}

		prepare(conn, expConn, *table, prepN, int(prepPhaseChunkSize), int(insertCommitsAfter))
	} else if *runPhase {
		glog.V(0).Info("Running 'run' phase!!!")
		conn := getConnection(*host, *user, *pwd, *db, *port)
		expConn := getConnection(*expHost, *user, *pwd, *db, *port)
		var prepN int
		var runN int
		if approxTableSize == 0 {
			glog.V(0).Info("APPROX_TABLE_SIZE has not been provided. Falling back to count rows from table")
			glog.V(0).Infof("Executing: SELECT COUNT(1) FROM %s", *table)
			var count int
			err := conn.QueryRow(fmt.Sprintf("SELECT COUNT(1) FROM %s", *table)).Scan(&count)
			if err != nil {
				glog.Fatal(err)
			}
			prepN = int(math.Round(tempTablePrepSizeRatio * float64(count)))
			tempTableRunSizeRatio = ((1 - tempTablePrepSizeRatio) * tempTableRunSizeRatio) + tempTablePrepSizeRatio
			runN = int(math.Round(tempTableRunSizeRatio * float64(count)))
			glog.V(2).Infof("preN: %d", prepN)
			glog.V(2).Infof("runN: %d", runN)
		} else {
			prepN = int(math.Round(tempTablePrepSizeRatio * float64(approxTableSize)))
			tempTableRunSizeRatio = ((1 - tempTablePrepSizeRatio) * tempTableRunSizeRatio) + tempTablePrepSizeRatio
			runN = int(math.Round(tempTableRunSizeRatio * float64(approxTableSize)))
			glog.V(2).Infof("preN: %d", prepN)
			glog.V(2).Infof("runN: %d", runN)
		}
		allowMissingIndex := map[string]bool{
			"read":   *allowMissingIndexRead,
			"update": *allowMissingIndexUpdate,
			"delete": *allowMissingIndexDelete,
		}
		run(int(runPhasePublishSleepTime), int(runPhaseSubscribeSleepTime), int(runPhasePublishChunkSize), 1, conn, expConn, *db, *table, allowMissingIndex, prepN, runN, *time, *createCPM, *readCPM, *updateCPM, *deleteCPM)

	} else {
		glog.V(0).Info("Neither prep nor run passed. Aborting!!!")
	}
}
