package main

import (
	"sync"

	"github.com/golang/glog"
)

func (m DesiredMetadata) read(queryType *string, typeOfData *string, data *interface{}) {
	var mutex sync.RWMutex
	if *typeOfData == "cpm" {
		mutex = dmCPMMutex[*queryType]
		glog.V(4).Info("Acquiring read lock for cpm for queryType: %s", *queryType)
		mutex.RLock()
		d := m.cpm[*queryType].(interface{})
		data = &d
	} else if *typeOfData == "wT" {
		mutex = dmWTMutex[*queryType]
		glog.V(4).Infof("Acquiring read lock for wT for queryType: %s", *queryType)
		mutex.RLock()
		d := m.wT[*queryType].(interface{})
		data = &d
	}
	glog.V(4).Infof("Releasing read lock for %s for queryType: %s", *typeOfData, *queryType)
	mutex.RUnlock()
	// TODO: use of RLock needs to be revisited. The difference between Lock and RLock needs to be clearly understood
}

func (m DesiredMetadata) write(queryType *string, typeOfData *string, data interface{}) {
	var mutex sync.RWMutex
	if *typeOfData == "cpm" {
		mutex = dmCPMMutex[*queryType]
		glog.V(4).Info("Acquiring lock for cpm for queryType: %s", *queryType)
		mutex.Lock()
		m.cpm[*queryType] = data
	} else if *typeOfData == "wT" {
		mutex = dmWTMutex[*queryType]
		glog.V(4).Infof("Acquiring lock for wT for queryType: %s", *queryType)
		mutex.Lock()
		m.wT[*queryType] = data
	}
	glog.V(4).Infof("Releasing lock for %s for queryType: %s", *typeOfData, *queryType)
	mutex.Unlock()
}

func (m RunMetadata) read(queryType *string, typeOfData *string, data *interface{}) {
	var mutex sync.RWMutex
	if *typeOfData == "cpm" {
		mutex = rmCPMMutex[*queryType]
		glog.V(4).Info("Acquiring read lock for cpm for queryType: %s", *queryType)
		mutex.RLock()
		d := m.cpm[*queryType].(interface{})
		data = &d
	} else if *typeOfData == "wT" {
		mutex = rmWTMutex[*queryType]
		glog.V(4).Infof("Acquiring read lock for wT for queryType: %s", *queryType)
		mutex.RLock()
		d := m.wT[*queryType].(interface{})
		data = &d
	}
	glog.V(4).Infof("Releasing read lock for %s for queryType: %s", *typeOfData, *queryType)
	mutex.RUnlock()
	// TODO: use of RLock needs to be revisited. The difference between Lock and RLock needs to be clearly understood
}

func (m RunMetadata) write(queryType *string, typeOfData *string, data interface{}) {
	var mutex sync.RWMutex
	if *typeOfData == "cpm" {
		mutex = rmCPMMutex[*queryType]
		glog.V(4).Info("Acquiring lock for cpm for queryType: %s", *queryType)
		mutex.Lock()
		m.cpm[*queryType] = data
	} else if *typeOfData == "wT" {
		mutex = rmWTMutex[*queryType]
		glog.V(4).Infof("Acquiring lock for wT for queryType: %s", *queryType)
		mutex.Lock()
		m.wT[*queryType] = data
	}
	glog.V(4).Infof("Releasing lock for %s for queryType: %s", *typeOfData, *queryType)
	mutex.Unlock()
}

func (m ControllerMetadata) read(queryType *string, typeOfData *string, data *interface{}) {
	var mutex sync.RWMutex
	if *typeOfData == "instances" {
		mutex = cmInstancesMutex[*queryType]
		glog.V(4).Info("Acquiring read lock for instances for queryType: %s", *queryType)
		mutex.RLock()
		d := m.instances[*queryType].(interface{})
		data = &d
	} else if *typeOfData == "chunk_size" {
		mutex = cmChunkSizeMutex[*queryType]
		glog.V(4).Infof("Acquiring read lock for chunk_size for queryType: %s", *queryType)
		mutex.RLock()
		d := m.chunkSize[*queryType].(interface{})
		data = &d
	} else if *typeOfData == "sleep_time" {
		mutex = cmSleepTimeMutex[*queryType]
		glog.V(4).Infof("Acquiring read lock for sleep_time for queryType: %s", *queryType)
		mutex.RLock()
		d := m.sleepTime[*queryType].(interface{})
		data = &d
	}

	glog.V(4).Infof("Releasing read lock for %s for queryType: %s", *typeOfData, *queryType)
	mutex.RUnlock()
	// TODO: use of RLock needs to be revisited. The difference between Lock and RLock needs to be clearly understood
}

func (m ControllerMetadata) write(queryType *string, typeOfData *string, data interface{}) {
	var mutex sync.RWMutex
	if *typeOfData == "instances" {
		mutex = cmInstancesMutex[*queryType]
		glog.V(4).Info("Acquiring lock for instances for queryType: %s", *queryType)
		mutex.Lock()
		m.instances[*queryType] = data
	} else if *typeOfData == "chunk_size" {
		mutex = cmChunkSizeMutex[*queryType]
		glog.V(4).Infof("Acquiring lock for chunk_size for queryType: %s", *queryType)
		mutex.Lock()
		m.chunkSize[*queryType] = data
	} else if *typeOfData == "sleep_time" {
		mutex = cmSleepTimeMutex[*queryType]
		glog.V(4).Infof("Acquiring lock for sleep_time for queryType: %s", *queryType)
		mutex.Lock()
		m.sleepTime[*queryType] = data
	}
	glog.V(4).Infof("Releasing lock for %s for queryType: %s", *typeOfData, *queryType)
	mutex.Unlock()
}
