package main

import (
	"database/sql"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/golang/glog"
)

func (mpc MasterPublishController) upscale(queryType *string, dM DesiredMetadata, dontCare *bool) bool {
	if *dontCare {
		return true
	}
	return false
}

func (mpc MasterPublishController) downscale(queryType *string, dM DesiredMetadata, dontCare *bool) bool {
	if *dontCare {
		return true
	}
	return false
}

func (msc MasterSubscribeController) upscale(queryType *string, dM DesiredMetadata, m MetadataTimeSeries, decisionWindow int, dontCare *bool, minSubscribeSleepTime int, maxSubscribeSleepTime int) bool {
	if *dontCare {
		return true
	}
	t := m.readLatest()
	currentCPM := t.cpm.(int)
	var desiredCPM interface{}
	typeOfData := "cpm"
	dM.read(queryType, &typeOfData, &desiredCPM)
	if currentCPM == 0 {
		if currentCPM < desiredCPM.(int) {
			return true
		}
		return false
	} else if currentCPM < desiredCPM.(int) {
		ratio := desiredCPM.(int) / currentCPM
		newSleepTime := int(msc.getSleepTime(queryType) / ratio)
		if newSleepTime < minSubscribeSleepTime {
			return true
		}
		msc.setSleepTime(queryType, newSleepTime)
	}
	return false
}

func (msc MasterSubscribeController) downscale(queryType *string, dM DesiredMetadata, m MetadataTimeSeries, decisionWindow int, dontCare *bool, minSubscribeSleepTime int, maxSubscribeSleepTime int) bool {
	if *dontCare {
		return true
	}
	t := m.readLatest()
	currentWT := t.wT.(int)
	maxWT := m.getMaxWT(decisionWindow)
	if currentWT > maxWT {
		return true
	}
	currentCPM := t.cpm.(int)
	if currentCPM == 0 {
		return false
	}
	var desiredCPM interface{}
	typeOfData := "cpm"
	dM.read(queryType, &typeOfData, &desiredCPM)
	if currentCPM > desiredCPM.(int) {
		ratio := currentCPM / desiredCPM.(int)
		newSleepTime := int(msc.getSleepTime(queryType) * ratio)
		if newSleepTime > maxSubscribeSleepTime {
			return true
		}
		msc.setSleepTime(queryType, newSleepTime)
	}
	return false
}

func (msc MasterSubscribeController) bombard(queryType *string, bus chan *sql.Rows, indexedCols map[string]bool, allowMissingIndex map[string]bool, qWT chan int, busEmpty chan string, maintainMinSubscribers chan bool, hardStopSignal chan bool, softStopSignal chan bool) {

	stopSubscriberDontCare := false

	defer decInstances(*queryType, msc.cM, maintainMinSubscribers, stopSubscriberDontCare)
	incInstances(*queryType, msc.cM, false)
	var r *sql.Rows
	var q Query
	breakLoop := false
	for {
		select {
		case r = <-bus:
			cols, _ := r.Columns()
			for r.Next() {
				select {
				case <-hardStopSignal:
					breakLoop = true
					stopSubscriberDontCare = true
					break
				default:
					break
				}
				select {
				case <-softStopSignal:
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
			if breakLoop {
				break
			}
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
func (mpc MasterPublishController) publishToBus(startID *int, count *int, bus chan *sql.Rows, maintainMinPublishers chan bool, hardStopSignal chan bool, softStopSignal chan bool) {
	queryType := "read"
	stopPublisherDontCare := false
	defer decInstances(queryType, mpc.cM, maintainMinPublishers, stopPublisherDontCare)
	incInstances(queryType, mpc.cM, false)
	source := rand.NewSource(time.Now().UnixNano())
	r := rand.New(source)
	breakLoop := false
	for {
		select {
		case <-hardStopSignal:
			breakLoop = true
			stopPublisherDontCare = true
			break
		case <-softStopSignal:
			breakLoop = true
			break
		default:
			offset := *startID + r.Intn(mpc.getChunkSize(&queryType))
			rows, err := mpc.db.Query(fmt.Sprintf("SELECT * FROM %s WHERE id >= %d LIMIT %d", *(mpc.tableName), offset, mpc.getChunkSize(&queryType)))
			if err != nil {
				glog.Info(err)
				return
			}
			bus <- rows
		}
		if breakLoop {
			break
		}
	}
}

func decInstances(queryType string, m Metadata, maintainMinPubSub chan bool, dontCare bool) {
	minPubSubInstances := 1
	typeOfData := "instances"
	var currentInstances interface{}
	m.read(&queryType, &typeOfData, &currentInstances)
	if (currentInstances.(int) <= minPubSubInstances) && !dontCare {
		maintainMinPubSub <- true
	}
	currentInstances = currentInstances.(int) - 1
	m.write(&queryType, &typeOfData, currentInstances)
}

func incInstances(queryType string, m Metadata, dontCare bool) {
	var currentInstances interface{}
	typeOfData := "instances"
	m.read(&queryType, &typeOfData, &currentInstances)
	currentInstances = currentInstances.(int) + 1
	m.write(&queryType, &typeOfData, currentInstances)
}

func (msc MasterSubscribeController) run(
	queryType string,
	dM DesiredMetadata,
	timeToRun int,
	indexedColumns map[string]bool,
	allowMissingIndex map[string]bool,
	timeSeries MetadataTimeSeries,
	timeSeriesTick int,
	pubSubComSignalSize int,
	minSubscribeSleepTime int,
	maxSubscribeSleepTime int,
	busEmpty chan string,
	bus chan *sql.Rows,
	qWT chan int,
	wg *sync.WaitGroup) {

	defer wg.Done()

	subscribeUpscaleDontCare := false
	subscribeDownscaleDontCare := false

	subscriberSoftStopSignal := make(chan bool, pubSubComSignalSize)
	subscriberHardStopSignal := make(chan bool, pubSubComSignalSize)

	stopMetricCompute := make(chan bool)
	maintainMinSubscribers := make(chan bool)

	relaxationTimeMS := timeSeriesTick + 10

	glog.V(1).Infof("Spawning computeMetric routine for queryType: %s", queryType)
	go computeMetrics(queryType, timeSeriesTick, timeSeries, qWT, stopMetricCompute)
	startTime := time.Now()

	for true {
		if (time.Now()).Sub(startTime).Minutes() > float64(timeToRun) {
			break
		}

		// select for maintainMinSubscribers
		select {
		case <-maintainMinSubscribers:
			subscribeUpscaleDontCare = true
			break
		default:
			break
		}

		canDownscale := msc.downscale(&queryType, dM, timeSeries, 20, &subscribeDownscaleDontCare, minSubscribeSleepTime, maxSubscribeSleepTime)
		canUpscale := msc.upscale(&queryType, dM, timeSeries, 20, &subscribeUpscaleDontCare, minSubscribeSleepTime, maxSubscribeSleepTime)

		if canDownscale && !subscribeUpscaleDontCare {
			glog.V(3).Info("downscaling subscriber instances by 1")
			subscriberSoftStopSignal <- true
		} else if canUpscale {
			glog.V(3).Info("upscaling subscriber instances by 1")
			go msc.bombard(&queryType, bus, indexedColumns, allowMissingIndex, qWT, busEmpty, maintainMinSubscribers, subscriberHardStopSignal, subscriberSoftStopSignal)
			subscribeUpscaleDontCare = false
		}
		time.Sleep(time.Duration(relaxationTimeMS) * time.Millisecond)
	}
	glog.V(1).Info("Cleaning up")
	glog.V(1).Info("Tearing down all subscriber instances")

	typeOfData := "instances"
	var currentInstances interface{}
	msc.cM.read(&queryType, &typeOfData, &currentInstances)
	for i := 0; i < currentInstances.(int); i++ {
		subscriberHardStopSignal <- true
	}
	glog.V(1).Info("Tore down all subscriber instances")
	glog.V(1).Infof("Flushing the bus")
	flushBus(bus)
	glog.V(1).Infof("Flushed the bus")
	glog.V(1).Infof("Preparing tear down of computeMetrics")
	// this is for allowing computeMetric to proceed from the qWT blocking call
	qWT <- 0

	glog.V(1).Info("Tearing down `computeMetrics` routine")
	stopMetricCompute <- true
	glog.V(1)
	close(qWT)
	glog.V(1).Infof("Tore down `computeMetrics` routine")
}

func (mpc MasterPublishController) run(
	queryType string,
	dM DesiredMetadata,
	timeToRun int,
	bus chan *sql.Rows,
	pubSubComSignalSize int,
	busEmpty chan string,
	wg *sync.WaitGroup,
	timeSeriesTick int,
	startID int,
	runChunk int) {

	defer wg.Done()

	publishUpscaleDontCare := false
	publishDownscaleDontCare := false

	var canUpscale bool
	var canDownscale bool

	publisherSoftStopSignal := make(chan bool, pubSubComSignalSize)
	publisherHardStopSignal := make(chan bool, pubSubComSignalSize)
	maintainMinPublishers := make(chan bool)

	startTime := time.Now()

	relaxationTimeMS := timeSeriesTick + 10

	glog.V(2).Infof("Starting initial publisher")
	go mpc.publishToBus(&startID, &runChunk, bus, maintainMinPublishers, publisherHardStopSignal, publisherSoftStopSignal)

	for true {
		if (time.Now()).Sub(startTime).Minutes() > float64(timeToRun) {
			break
		}
		// select for maintainMinPublishers
		select {
		case <-maintainMinPublishers:
			publishUpscaleDontCare = true
			break
		default:
			break
		}
		// select for busEmpty
		select {
		case <-busEmpty:
			glog.V(3).Info("Bus found to be empty")
			publishUpscaleDontCare = true
			break
		default:
			break
		}
		canDownscale = mpc.downscale(&queryType, dM, &publishDownscaleDontCare)
		canUpscale = mpc.upscale(&queryType, dM, &publishUpscaleDontCare)

		if canDownscale && !publishUpscaleDontCare {
			glog.V(3).Info("downscaling publisher instances by 1")
			publisherSoftStopSignal <- true
		} else if canUpscale {
			glog.V(3).Info("upscaling publisher instances by 1")
			go mpc.publishToBus(&startID, &runChunk, bus, maintainMinPublishers, publisherHardStopSignal, publisherSoftStopSignal)
			publishUpscaleDontCare = false
		}
		time.Sleep(time.Duration(relaxationTimeMS) * time.Millisecond)
	}
	glog.V(1).Info("Tearing down all publisher instances")

	typeOfData := "instances"
	var currentInstances interface{}
	mpc.cM.read(&queryType, &typeOfData, &currentInstances)
	for i := 0; i < currentInstances.(int); i++ {
		publisherHardStopSignal <- true
	}
	glog.V(1).Info("Tore down all publisher instances")
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
