package main

import (
	"sync"

	"github.com/golang/glog"
)

func (m DesiredMetadata) read(queryType *string, typeOfData *string, data *interface{}) {
	var mutex *sync.RWMutex
	if *typeOfData == "cpm" {
		mutex = dmCPMMutex[*queryType]
		glog.V(4).Infof("Acquiring read lock for cpm for queryType: %s", *queryType)
		mutex.RLock()
		defer mutex.RUnlock()
		d := m.cpm[*queryType].(interface{})
		*data = d
	}
	glog.V(4).Infof("Releasing read lock for %s for queryType: %s", *typeOfData, *queryType)

}

func (m DesiredMetadata) write(queryType *string, typeOfData *string, data interface{}) {
	var mutex *sync.RWMutex
	if *typeOfData == "cpm" {
		mutex = dmCPMMutex[*queryType]
		glog.V(4).Infof("Acquiring lock for cpm for queryType: %s", *queryType)
		mutex.Lock()
		defer mutex.Unlock()
		m.cpm[*queryType] = data
	}
	glog.V(4).Infof("Releasing lock for %s for queryType: %s", *typeOfData, *queryType)
}

func (m ControllerMetadata) read(queryType *string, typeOfData *string, data *interface{}) {
	var mutex *sync.RWMutex
	if *typeOfData == "instances" {
		if m.controllerType == "publish" {
			mutex = pcmInstancesMutex[*queryType]
		} else {
			mutex = scmInstancesMutex[*queryType]
		}
		glog.V(4).Infof("Acquiring read lock for %s instances for queryType: %s", m.controllerType, *queryType)
		mutex.RLock()
		defer mutex.RUnlock()
		d := m.instances[*queryType].(interface{})
		*data = d
	} else if *typeOfData == "chunk_size" {
		if m.controllerType == "publish" {
			mutex = pcmChunkSizeMutex[*queryType]
		} else {
			mutex = scmChunkSizeMutex[*queryType]
		}
		glog.V(4).Infof("Acquiring read lock for %s chunk_size for queryType: %s", m.controllerType, *queryType)
		mutex.RLock()
		defer mutex.RUnlock()
		d := m.chunkSize[*queryType].(interface{})
		*data = d
	} else if *typeOfData == "sleep_time" {
		if m.controllerType == "publish" {
			mutex = pcmSleepTimeMutex[*queryType]
		} else {
			mutex = scmSleepTimeMutex[*queryType]
		}
		glog.V(4).Infof("Acquiring read lock for %s sleep_time for queryType: %s", m.controllerType, *queryType)
		mutex.RLock()
		defer mutex.RUnlock()
		d := m.sleepTime[*queryType].(interface{})
		*data = d
	}
	glog.V(4).Infof("Releasing read lock for %s %s for queryType: %s", m.controllerType, *typeOfData, *queryType)
	// TODO: use of RLock needs to be revisited. The difference between Lock and RLock needs to be clearly understood
}

func (m ControllerMetadata) write(queryType *string, typeOfData *string, data interface{}) {
	var mutex *sync.RWMutex
	if *typeOfData == "instances" {
		if m.controllerType == "publish" {
			mutex = pcmInstancesMutex[*queryType]
		} else {
			mutex = scmInstancesMutex[*queryType]
		}
		glog.V(4).Infof("Acquiring lock for %s instances for queryType: %s", m.controllerType, *queryType)
		mutex.Lock()
		defer mutex.Unlock()
		m.instances[*queryType] = data
	} else if *typeOfData == "chunk_size" {
		if m.controllerType == "publish" {
			mutex = pcmChunkSizeMutex[*queryType]
		} else {
			mutex = scmChunkSizeMutex[*queryType]
		}
		glog.V(4).Infof("Acquiring lock for %s chunk_size for queryType: %s", m.controllerType, *queryType)
		mutex.Lock()
		defer mutex.Unlock()
		m.chunkSize[*queryType] = data
	} else if *typeOfData == "sleep_time" {
		if m.controllerType == "publish" {
			mutex = pcmSleepTimeMutex[*queryType]
		} else {
			mutex = scmSleepTimeMutex[*queryType]
		}
		glog.V(4).Infof("Acquiring lock for %s sleep_time for queryType: %s", m.controllerType, *queryType)
		mutex.Lock()
		defer mutex.Unlock()
		m.sleepTime[*queryType] = data
	}
	glog.V(4).Infof("Releasing lock for %s %s for queryType: %s", m.controllerType, *typeOfData, *queryType)
}
