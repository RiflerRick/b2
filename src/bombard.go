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
	"sync"

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
RunPhaseConfig is the configs supplied by the user for the run phase
*/
type RunPhaseConfig struct {
	createCPM           interface{}
	readCPM             interface{}
	updateCPM           interface{}
	deleteCPM           interface{}
	publishChunkSize    interface{}
	subscribeChunkSize  interface{}
	publishSleepTime    interface{}
	subscribeSleepTime  interface{}
	timeSeriesTick      interface{}
	timeSeriesSize      interface{}
	pubSubComSignalSize interface{}
	metricPollTick      interface{}
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

var (
	createMetadataTimeSeriesMutex sync.RWMutex
	readMetadataTimeSeriesMutex   sync.RWMutex
	updateMetadataTimeSeriesMutex sync.RWMutex
	deleteMetadataTimeSeriesMutex sync.RWMutex
)

/*
MetadataTimeSeries for storing all metadata types
*/
type MetadataTimeSeries interface {
	readLatest() timeSeriesPoint
	write(t timeSeriesPoint) error
	getMaxWT(decisionWindow int) int
}

type createMetadataTimeSeries struct {
	data []timeSeriesPoint
}

type readMetadataTimeSeries struct {
	data []timeSeriesPoint
}

type updateMetadataTimeSeries struct {
	data []timeSeriesPoint
}

type deleteMetadataTimeSeries struct {
	data []timeSeriesPoint
}

// mutex cannot obviously be part of the resource being shared
// TODO: instead of declaring variables of type sync.RWMutex, there must be a better way to do this

var timeSeriesMutex sync.RWMutex

var (
	createDMCPMMutex sync.RWMutex
	readDMCPMMutex   sync.RWMutex
	updateDMCPMMutex sync.RWMutex
	deleteDMCPMMutex sync.RWMutex

	// instances mutexes
	createPCMInstancesMutex sync.RWMutex
	readPCMInstancesMutex   sync.RWMutex
	updatePCMInstancesMutex sync.RWMutex
	deletePCMInstancesMutex sync.RWMutex

	createSCMInstancesMutex sync.RWMutex
	readSCMInstancesMutex   sync.RWMutex
	updateSCMInstancesMutex sync.RWMutex
	deleteSCMInstancesMutex sync.RWMutex

	// sleep mutexes
	createPCMSleepMutex sync.RWMutex
	readPCMSleepMutex   sync.RWMutex
	updatePCMSleepMutex sync.RWMutex
	deletePCMSleepMutex sync.RWMutex

	createSCMSleepMutex sync.RWMutex
	readSCMSleepMutex   sync.RWMutex
	updateSCMSleepMutex sync.RWMutex
	deleteSCMSleepMutex sync.RWMutex

	// chunk mutexes
	createPCMChunkMutex sync.RWMutex
	readPCMChunkMutex   sync.RWMutex
	updatePCMChunkMutex sync.RWMutex
	deletePCMChunkMutex sync.RWMutex

	createSCMChunkMutex sync.RWMutex
	readSCMChunkMutex   sync.RWMutex
	updateSCMChunkMutex sync.RWMutex
	deleteSCMChunkMutex sync.RWMutex
)

var (
	dmCPMMutex map[string]*sync.RWMutex

	pcmInstancesMutex map[string]*sync.RWMutex
	pcmSleepTimeMutex map[string]*sync.RWMutex
	pcmChunkSizeMutex map[string]*sync.RWMutex

	scmInstancesMutex map[string]*sync.RWMutex
	scmSleepTimeMutex map[string]*sync.RWMutex
	scmChunkSizeMutex map[string]*sync.RWMutex
)

type timeSeriesPoint struct {
	cpm interface{}
	wT  interface{}
}

/*
DesiredMetadata will never change. contains calls per minute and wait time
For each type, the keys of the map will be the querytypes
*/
type DesiredMetadata struct {
	cpm map[string]interface{}
}

/*
ControllerMetadata is the metadata maintained by MasterPublishController and MasterSubscribeController
For each type, the keys of the map will be the querytypes
*/
type ControllerMetadata struct {
	controllerType string
	instances      map[string]interface{}
	chunkSize      map[string]interface{}
	sleepTime      map[string]interface{}
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

/*
dM: desiredMetadata consisting of CPM, wT //this never changes
rM: runMetadata consisting of CPM, wT (actual)
cM: controllerMetadata consisting of instances_running, sleepTime and chunkSize
*/
func run(c RunPhaseConfig, db *sql.DB, expDB *sql.DB, tableSchema string, tableName string, allowMissingIndex map[string]bool, prepN int, runN int, time int) {
	pcmInstancesMutex = map[string]*sync.RWMutex{
		"create": &createPCMInstancesMutex,
		"read":   &readPCMInstancesMutex,
		"update": &updatePCMInstancesMutex,
		"delete": &deletePCMInstancesMutex,
	}

	pcmChunkSizeMutex = map[string]*sync.RWMutex{
		"create": &createPCMChunkMutex,
		"read":   &readPCMChunkMutex,
		"update": &updatePCMChunkMutex,
		"delete": &deletePCMChunkMutex,
	}

	pcmSleepTimeMutex = map[string]*sync.RWMutex{
		"create": &createPCMSleepMutex,
		"read":   &readPCMSleepMutex,
		"update": &updatePCMSleepMutex,
		"delete": &deletePCMSleepMutex,
	}

	scmInstancesMutex = map[string]*sync.RWMutex{
		"create": &createSCMInstancesMutex,
		"read":   &readSCMInstancesMutex,
		"update": &updateSCMInstancesMutex,
		"delete": &deleteSCMInstancesMutex,
	}

	scmChunkSizeMutex = map[string]*sync.RWMutex{
		"create": &createSCMChunkMutex,
		"read":   &readSCMChunkMutex,
		"update": &updateSCMChunkMutex,
		"delete": &deleteSCMChunkMutex,
	}

	scmSleepTimeMutex = map[string]*sync.RWMutex{
		"create": &createSCMSleepMutex,
		"read":   &readSCMSleepMutex,
		"update": &updateSCMSleepMutex,
		"delete": &deleteSCMSleepMutex,
	}

	dmCPMMutex = map[string]*sync.RWMutex{
		"create": &createDMCPMMutex,
		"read":   &readDMCPMMutex,
		"update": &updateDMCPMMutex,
		"delete": &deleteDMCPMMutex,
	}

	var mpc MasterPublishController
	var msc MasterSubscribeController

	mpc.cM.controllerType = "publish"
	msc.cM.controllerType = "subscribe"

	mpc.cM.instances = map[string]interface{}{
		"create": 0,
		"read":   0,
		"update": 0,
		"delete": 0,
	}
	mpc.cM.sleepTime = map[string]interface{}{
		"create": c.publishSleepTime,
		"read":   c.publishSleepTime,
		"update": c.publishSleepTime,
		"delete": c.publishSleepTime,
	}
	mpc.cM.chunkSize = map[string]interface{}{
		"create": c.publishChunkSize,
		"read":   c.publishChunkSize,
		"update": c.publishChunkSize,
		"delete": c.publishChunkSize,
	}
	msc.cM.instances = map[string]interface{}{
		"create": 0,
		"read":   0,
		"update": 0,
		"delete": 0,
	}

	msc.cM.sleepTime = map[string]interface{}{
		"create": c.subscribeSleepTime,
		"read":   c.subscribeSleepTime,
		"update": c.subscribeSleepTime,
		"delete": c.subscribeSleepTime,
	}
	msc.cM.chunkSize = map[string]interface{}{
		"create": c.subscribeChunkSize,
		"read":   c.subscribeChunkSize,
		"update": c.subscribeChunkSize,
		"delete": c.subscribeChunkSize,
	}

	expTableName := fmt.Sprintf("%s_c4", tableName)
	mpc.tableName = &tableName
	msc.tableName = &expTableName
	mpc.db = db
	msc.db = expDB

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

	desiredMetadata.cpm = map[string]interface{}{
		"create": int(c.createCPM.(int) / 60),
		"read":   int(c.readCPM.(int) / 60),
		"update": int(c.updateCPM.(int) / 60),
		"delete": int(c.deleteCPM.(int) / 60),
	}

	glog.V(1).Infof("Starting publishers")
	var wg sync.WaitGroup
	busEmpty := make(chan string)

	bus := make(chan *sql.Rows)

	wg.Add(5)

	var createTimeSeries createMetadataTimeSeries
	var readTimeSeries readMetadataTimeSeries
	var updateTimeSeries updateMetadataTimeSeries
	var deleteTimeSeries deleteMetadataTimeSeries

	createTimeSeries.data = make([]timeSeriesPoint, 0)
	readTimeSeries.data = make([]timeSeriesPoint, 0)
	updateTimeSeries.data = make([]timeSeriesPoint, 0)
	deleteTimeSeries.data = make([]timeSeriesPoint, 0)

	go mpc.run(
		"read",
		desiredMetadata,
		time,
		bus,
		c.pubSubComSignalSize.(int),
		busEmpty,
		&wg,
		c.timeSeriesTick.(int),
		startID,
		count)

	createQWT := make(chan int)
	readQWT := make(chan int)
	updateQWT := make(chan int)
	deleteQWT := make(chan int)

	stopPoll := make(chan bool)

	glog.V(0).Info("Polling metrics:")

	go allMetricPoll(c.metricPollTick.(int), desiredMetadata, mpc.cM, msc.cM, &createTimeSeries, &readTimeSeries, &updateTimeSeries, &deleteTimeSeries, stopPoll)

	go glog.V(1).Info("Starting subscribers")

	//queryType string, dM DesiredMetadata, timeToRun int, indexedColumns map[string]bool, allowMissingIndex map[string]bool, timeSeries []timeSeriesPoint, timeSeriesTick int, pubSubComSignalSize int, busEmpty chan string, bus chan *sql.Rows, qWT chan int, wg *sync.WaitGroup
	go msc.run(
		"create",
		desiredMetadata,
		time,
		indexedColumnsMap,
		allowMissingIndex,
		&createTimeSeries,
		c.timeSeriesTick.(int),
		c.pubSubComSignalSize.(int),
		busEmpty,
		bus,
		createQWT,
		&wg)
	go msc.run(
		"read",
		desiredMetadata,
		time,
		indexedColumnsMap,
		allowMissingIndex,
		&readTimeSeries,
		c.timeSeriesTick.(int),
		c.pubSubComSignalSize.(int),
		busEmpty,
		bus,
		readQWT,
		&wg)
	go msc.run(
		"update",
		desiredMetadata,
		time,
		indexedColumnsMap,
		allowMissingIndex,
		&updateTimeSeries,
		c.timeSeriesTick.(int),
		c.pubSubComSignalSize.(int),
		busEmpty,
		bus,
		updateQWT,
		&wg)
	go msc.run(
		"delete",
		desiredMetadata,
		time,
		indexedColumnsMap,
		allowMissingIndex,
		&deleteTimeSeries,
		c.timeSeriesTick.(int),
		c.pubSubComSignalSize.(int),
		busEmpty,
		bus,
		deleteQWT,
		&wg)
	glog.V(1).Info("Waiting for MasterPublishController and MasterSubscribeController to finish")
	wg.Wait()

	glog.V(1).Info("Stoppinig all metric poll")
	stopPoll <- true

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
	pubSubComSignalSize, _ := strconv.ParseInt(os.Getenv("PUB_SUB_COMM_SIGNAL_SIZE"), 10, 0)
	metricPollTick, _ := strconv.ParseInt(os.Getenv("METRIC_POLL_TICK_MS"), 10, 0)

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
	if pubSubComSignalSize == 0 {
		defVal := 100
		glog.V(0).Infof("PUB_SUB_COMM_SIGNAL_SIZE has not been set, defaulting to %d", defVal)
		pubSubComSignalSize = int64(pubSubComSignalSize)
	}
	if metricPollTick == 0 {
		defVal := 1000
		glog.V(0).Infof("METRIC_POLL_TICK_MS has not been set, defaulting to %d", defVal)
		metricPollTick = int64(metricPollTick)
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
		timeSeriesTick := 1000

		var c RunPhaseConfig
		c.createCPM = *createCPM
		c.readCPM = *readCPM
		c.updateCPM = *updateCPM
		c.deleteCPM = *deleteCPM
		c.publishChunkSize = int(runPhasePublishChunkSize)
		c.publishSleepTime = int(runPhasePublishSleepTime)
		c.subscribeChunkSize = 1
		c.subscribeSleepTime = int(runPhaseSubscribeSleepTime)
		c.timeSeriesTick = int(timeSeriesTick)
		c.pubSubComSignalSize = int(pubSubComSignalSize)
		c.metricPollTick = int(metricPollTick)

		run(c, conn, expConn, *db, *table, allowMissingIndex, prepN, runN, *time)

	} else {
		glog.V(0).Info("Neither prep nor run passed. Aborting!!!")
	}
}
