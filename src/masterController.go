package main

import (
	"database/sql"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"sync"
	"time"

	"github.com/golang/glog"
)

func (mpc MasterPublishController) upscale(queryType *string, dM DesiredMetadata, rM RunMetadata, dontCare *bool) bool {
	if *dontCare {
		return true
	}
	return false
}

func (msc MasterSubscribeController) balance(queryType *string, dM DesiredMetadata, rM RunMetadata, dontCare *bool) string {
	typeOfData := "cpm"
	var rCPM interface{}
	var dCPM interface{}
	rM.read(queryType, &typeOfData, &rCPM)
	dM.read(queryType, &typeOfData, &dCPM)

	var avgDMWT interface{}
	wT := "wT"
	dM.read(queryType, &wT, &avgDMWT)
	var avgRMWT interface{}
	rM.read(queryType, &wT, &avgRMWT)
	dmWT, ok := avgDMWT.(int)

	if !ok {
		// its possible the number is infinite which is of type float64
		if reflect.TypeOf(avgDMWT).String() == "float64" && math.IsInf(avgDMWT.(float64), 1) {
			return "default"
		}
	}
	rmWT, ok := avgRMWT.(int)
	if !ok {
		// its possible the number is infinite which is of type float64
		if reflect.TypeOf(avgRMWT).String() == "float64" && math.IsInf(avgRMWT.(float64), 1) {
			return "default"
		}
	}

	if rCPM.(int) < dCPM.(int) {
		if rmWT < dmWT {
			return "aggressiveUpscale"
		}
		return "mildDownscale"
	}
	if rmWT < dmWT {
		return "mildDownscale"
	}
	return "aggressiveDownscale"
}

func (mpc MasterPublishController) downscale(queryType *string, dM DesiredMetadata, rM RunMetadata, dontCare *bool) bool {
	// TODO: needs to be worked upon
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

	createDM, ok := createVal.(int)
	if !ok {
		if reflect.TypeOf(createVal).String() == "float64" && math.IsInf(createVal.(float64), 1) {
			return false
		}
		panic(ok)
	}

	readDM, ok := readVal.(int)
	if !ok {
		if reflect.TypeOf(readVal).String() == "float64" && math.IsInf(readVal.(float64), 1) {
			return false
		}
		panic(ok)
	}

	updateDM, ok := updateVal.(int)
	if !ok {
		if reflect.TypeOf(updateVal).String() == "float64" && math.IsInf(updateVal.(float64), 1) {
			return false
		}
		panic(ok)
	}

	deleteDM, ok := deleteVal.(int)
	if !ok {
		if reflect.TypeOf(deleteVal).String() == "float64" && math.IsInf(deleteVal.(float64), 1) {
			return false
		}
		panic(ok)
	}

	sum = createDM + readDM + updateDM + deleteDM
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

/*
subscribe to bus for hitting the db. sleep decides how much to sleep in between queries
queryTypeCPM: map storing the CPM values for each query. The type of query to be fired will be chosen by this CPM
*/
func (msc MasterSubscribeController) bombard(queryType *string, bus chan *sql.Rows, indexedCols map[string]bool, allowMissingIndex map[string]bool, qWT chan int, busEmpty chan string, stopSignal chan bool) {
	defer decInstances(*queryType, msc.cM)
	incInstances(*queryType, msc.cM)
	var r *sql.Rows
	var q Query
	breakLoop := false
	for {
		select {
		case r = <-bus:
			cols, _ := r.Columns()
			for r.Next() {
				select {
				case <-stopSignal:
					breakLoop = true
					break
				default:
					break
				}
				if breakLoop {
					break
				}
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

				query, columnData := getQuery(queryType, msc.tableName, msc.getChunkSize(queryType), colData, indexedCols, allowMissingIndex)
				if *queryType == "read" {
					q.query = query
					rows := q.executeRead(msc.db, columnData...)
					rows.Close()
					qWT <- q.wt
				} else {
					q.query = query
					q.executeWrite(msc.db, columnData...)
					qWT <- q.wt
				}

				time.Sleep(time.Millisecond * time.Duration(msc.getSleepTime(queryType)))
			}
			r.Close()
		default:
			// bus should never be empty
			busEmpty <- *queryType
		}
		if breakLoop {
			break
		}

	}
}

/*
	function to publish data to the bus after reading from the source db
	to be called as a go routine. publishes data to the bus channel to be consumed by bombarding routines
*/
func (mpc MasterPublishController) publishToBus(startID *int, count *int, bus chan *sql.Rows, stopSignal chan bool) {
	queryType := "read"
	defer decInstances(queryType, mpc.cM)
	incInstances(queryType, mpc.cM)
	source := rand.NewSource(time.Now().UnixNano())
	r := rand.New(source)
	breakLoop := false
	for {
		select {
		case <-stopSignal:
			breakLoop = true
		default:
			offset := r.Intn(*count)
			rows, err := mpc.db.Query(fmt.Sprintf("SELECT * FROM %s WHERE id >= %d LIMIT %d OFFSET %d", *(mpc.tableName), *startID, mpc.getChunkSize(&queryType), offset))
			if err != nil {
				glog.Info(err)
				return
			}
			bus <- rows
			time.Sleep(time.Duration(mpc.getSleepTime(&queryType)) * time.Millisecond)
		}
		if breakLoop {
			break
		}
	}
}

func decInstances(queryType string, m Metadata) {
	var currentInstances interface{}
	typeOfData := "instances"
	m.read(&queryType, &typeOfData, &currentInstances)
	currentInstances = currentInstances.(int) - 1
	m.write(&queryType, &typeOfData, currentInstances)
}

func incInstances(queryType string, m Metadata) {
	var currentInstances interface{}
	typeOfData := "instances"
	m.read(&queryType, &typeOfData, &currentInstances)
	currentInstances = currentInstances.(int) + 1
	m.write(&queryType, &typeOfData, currentInstances)
}

func (msc MasterSubscribeController) run(queryType string, dM DesiredMetadata, rM RunMetadata, timeToRun int, indexedColumns map[string]bool, allowMissingIndex map[string]bool, busEmpty chan string, bus chan *sql.Rows, qWT chan int, wg *sync.WaitGroup) {
	defer wg.Done()

	relaxationTimeInMS := 500

	subscribeDontCare := false
	var verdict string

	maxSubscriberCountExpected := 100
	subscriberStopSignal := make(chan bool, maxSubscriberCountExpected)

	stopMetricCompute := make(chan bool)

	glog.V(1).Infof("Spawning computeMetric routine for queryType: %s", queryType)
	go computeMetrics(queryType, rM, qWT, msc.cM, stopMetricCompute)
	startTime := time.Now()

	var currentInstances interface{}
	typeOfData := "instances"
	for true {
		if (time.Now()).Sub(startTime).Minutes() > float64(timeToRun) {
			break
		}
		verdict = msc.balance(&queryType, dM, rM, &subscribeDontCare)

		switch verdict {
		case "aggressiveUpscale":
			glog.V(3).Infof("upscaling subscriber instances by one for queryType: %s", queryType)
			go msc.bombard(&queryType, bus, indexedColumns, allowMissingIndex, qWT, busEmpty, subscriberStopSignal)
		case "aggressiveDownscale":
			glog.V(3).Infof("downscaling subscriber instances by one for queryType: %s", queryType)
			subscriberStopSignal <- true
		case "mildDownscale":
			// increase the sleep time
			deltaSleepTime := 100
			glog.V(3).Infof("increasing subscriber sleep time for queryType: %s by %d", queryType, deltaSleepTime)
			currentSleepTime := msc.getSleepTime(&queryType)
			msc.setSleepTime(&queryType, currentSleepTime+deltaSleepTime)
		default:
			glog.V(3).Infof("No decision taken for subscriber for queryType: %s", queryType)
			break
		}
		time.Sleep(time.Duration(relaxationTimeInMS) * time.Millisecond)
	}
	glog.V(1).Info("Tearing down all subscriber instances")
	msc.cM.read(&queryType, &typeOfData, &currentInstances)
	for i := 0; i < currentInstances.(int); i++ {
		subscriberStopSignal <- true
	}
	glog.V(1).Info("Tearing down `computeMetrics` routine")
	stopMetricCompute <- true
}

func (mpc MasterPublishController) run(queryType string, dM DesiredMetadata, rM RunMetadata, timeToRun int, bus chan *sql.Rows, busEmpty chan string, wg *sync.WaitGroup, startID int, runChunk int) {
	defer wg.Done()

	relaxationTimeInMS := 500

	publishDontCare := false
	var canUpscale bool
	var canDownscale bool

	maxPublisherCountExpected := 100
	publisherStopSignal := make(chan bool, maxPublisherCountExpected)

	startTime := time.Now()
	var currentInstances interface{}
	typeOfData := "instances"
	for true {
		if (time.Now()).Sub(startTime).Minutes() > float64(timeToRun) {
			break
		}
		select {
		case <-busEmpty:
			glog.V(3).Info("Bus found to be empty")
			publishDontCare = true
		default:
			// in case the bus is empty, publish will happen only after one tick
			canDownscale = mpc.downscale(&queryType, dM, rM, &publishDontCare)
			canUpscale = mpc.upscale(&queryType, dM, rM, &publishDontCare)
			publishDontCare = false

			if canDownscale {
				glog.V(3).Info("downscaling publisher instances by 1")
				mpc.cM.read(&queryType, &typeOfData, &currentInstances)
				currentInstances = currentInstances.(int) - 1
				mpc.cM.write(&queryType, &typeOfData, currentInstances)
				publisherStopSignal <- true
			} else if canUpscale {
				glog.V(3).Info("upscaling publisher instances by 1")
				mpc.cM.read(&queryType, &typeOfData, &currentInstances)
				currentInstances = currentInstances.(int) + 1
				mpc.cM.write(&queryType, &typeOfData, currentInstances)
				go mpc.publishToBus(&startID, &runChunk, bus, publisherStopSignal)
			}
			// relax for a few milliseconds
			time.Sleep(time.Duration(relaxationTimeInMS) * time.Millisecond)
		}
	}
	glog.V(1).Info("Tearing down all publisher instances")
	mpc.cM.read(&queryType, &typeOfData, &currentInstances)
	for i := 0; i < currentInstances.(int); i++ {
		publisherStopSignal <- true
	}
}

func (mpc MasterPublishController) getChunkSize(queryType *string) int {
	// TODO: more intelligence required
	var chunkSize interface{}
	typeOfData := "chunk_size"
	mpc.cM.read(queryType, &typeOfData, &chunkSize)
	return chunkSize.(int)
}

func (mpc MasterPublishController) getSleepTime(queryType *string) int {
	// TODO: more intelligence required
	var sleepTime interface{}
	typeOfData := "sleep_time"
	mpc.cM.read(queryType, &typeOfData, &sleepTime)
	return sleepTime.(int)
}

func (mpc MasterPublishController) setChunkSize(queryType *string, data interface{}) {
	typeOfData := "chunk_size"
	mpc.cM.write(queryType, &typeOfData, data)
}

func (mpc MasterPublishController) setSleepTime(queryType *string, data interface{}) {
	typeOfData := "sleep_time"
	mpc.cM.write(queryType, &typeOfData, data)
}

func (msc MasterSubscribeController) getSleepTime(queryType *string) int {
	// TODO: more intelligence required
	var sleepTime interface{}
	typeOfData := "sleep_time"
	msc.cM.read(queryType, &typeOfData, &sleepTime)
	return sleepTime.(int)
}

func (msc MasterSubscribeController) getChunkSize(queryType *string) int {
	// TODO: more intelligence required
	var chunkSize interface{}
	typeOfData := "chunk_size"
	msc.cM.read(queryType, &typeOfData, &chunkSize)
	return chunkSize.(int)
}

func (msc MasterSubscribeController) setSleepTime(queryType *string, data interface{}) {
	typeOfData := "sleep_time"
	msc.cM.write(queryType, &typeOfData, data)
}

func (msc MasterSubscribeController) setChunkSize(queryType *string, data interface{}) {
	typeOfData := "chunk_size"
	msc.cM.write(queryType, &typeOfData, data)
}
