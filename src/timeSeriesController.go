package main

import "errors"

func (t *createMetadataTimeSeries) readLatest() (timeSeriesPoint, error) {
	//get the last timeSeriesPoint that was inserted
	createMetadataTimeSeriesMutex.RLock()
	defer createMetadataTimeSeriesMutex.RUnlock()
	if len(t.data) == 0 {
		return timeSeriesPoint{cpm: -1, wT: -1}, errors.New("length of time series is 0")
	}
	data := t.data[len(t.data)-1]
	return data, nil
}

func (t *readMetadataTimeSeries) readLatest() (timeSeriesPoint, error) {
	// get the last timeSeriesPoint that was inserted
	readMetadataTimeSeriesMutex.RLock()
	defer readMetadataTimeSeriesMutex.RUnlock()
	if len(t.data) == 0 {
		return timeSeriesPoint{cpm: -1, wT: -1}, errors.New("length of time series is 0")
	}
	data := t.data[len(t.data)-1]
	return data, nil
}

func (t *updateMetadataTimeSeries) readLatest() (timeSeriesPoint, error) {
	updateMetadataTimeSeriesMutex.RLock()
	defer updateMetadataTimeSeriesMutex.RUnlock()
	if len(t.data) == 0 {
		return timeSeriesPoint{cpm: -1, wT: -1}, errors.New("length of time series is 0")
	}
	data := t.data[len(t.data)-1]
	return data, nil
}

func (t *deleteMetadataTimeSeries) readLatest() (timeSeriesPoint, error) {
	deleteMetadataTimeSeriesMutex.RLock()
	defer deleteMetadataTimeSeriesMutex.RUnlock()
	if len(t.data) == 0 {
		return timeSeriesPoint{cpm: -1, wT: -1}, errors.New("length of time series is 0")
	}
	data := t.data[len(t.data)-1]
	return data, nil
}

func (s *createMetadataTimeSeries) write(t timeSeriesPoint) error {
	createMetadataTimeSeriesMutex.Lock()
	defer createMetadataTimeSeriesMutex.Unlock()
	s.data = append(s.data, t)
	return nil
}

func (s *readMetadataTimeSeries) write(t timeSeriesPoint) error {
	readMetadataTimeSeriesMutex.Lock()
	s.data = append(s.data, t)
	readMetadataTimeSeriesMutex.Unlock()
	return nil
}

func (s *updateMetadataTimeSeries) write(t timeSeriesPoint) error {
	updateMetadataTimeSeriesMutex.Lock()
	s.data = append(s.data, t)
	updateMetadataTimeSeriesMutex.Unlock()
	return nil
}

func (s *deleteMetadataTimeSeries) write(t timeSeriesPoint) error {
	deleteMetadataTimeSeriesMutex.Lock()
	s.data = append(s.data, t)
	deleteMetadataTimeSeriesMutex.Unlock()
	return nil
}

func (t *createMetadataTimeSeries) getMaxWT(decisionWindow int) (int, error) {
	maxValue := 0
	createMetadataTimeSeriesMutex.RLock()
	defer createMetadataTimeSeriesMutex.RUnlock()
	tLen := len(t.data)
	if tLen == 0 {
		return -1, errors.New("length of time series is 0")
	}
	if tLen < decisionWindow {
		decisionWindow = tLen
	}
	for i := 0; i < decisionWindow; i++ {
		valueNow := t.data[tLen-i-1].wT.(int)
		if valueNow > maxValue {
			maxValue = valueNow
		}
	}
	return maxValue, nil
}

func (t *readMetadataTimeSeries) getMaxWT(decisionWindow int) (int, error) {
	maxValue := 0
	readMetadataTimeSeriesMutex.RLock()
	defer readMetadataTimeSeriesMutex.RUnlock()
	tLen := len(t.data)
	if tLen == 0 {
		return -1, errors.New("length of time series is 0")
	}
	if tLen < decisionWindow {
		decisionWindow = tLen
	}
	for i := 0; i < decisionWindow; i++ {
		valueNow := t.data[tLen-i-1].wT.(int)
		if valueNow > maxValue {
			maxValue = valueNow
		}
	}
	return maxValue, nil
}

func (t *updateMetadataTimeSeries) getMaxWT(decisionWindow int) (int, error) {
	maxValue := 0
	updateMetadataTimeSeriesMutex.RLock()
	defer updateMetadataTimeSeriesMutex.RUnlock()
	tLen := len(t.data)
	if tLen == 0 {
		return -1, errors.New("length of time series is 0")
	}
	if tLen < decisionWindow {
		decisionWindow = tLen
	}
	for i := 0; i < decisionWindow; i++ {
		valueNow := t.data[tLen-i-1].wT.(int)
		if valueNow > maxValue {
			maxValue = valueNow
		}
	}
	return maxValue, nil
}

func (t *deleteMetadataTimeSeries) getMaxWT(decisionWindow int) (int, error) {
	maxValue := 0
	deleteMetadataTimeSeriesMutex.RLock()
	defer deleteMetadataTimeSeriesMutex.RUnlock()
	tLen := len(t.data)
	if tLen == 0 {
		return -1, errors.New("length of time series is 0")
	}
	if tLen < decisionWindow {
		decisionWindow = tLen
	}
	for i := 0; i < decisionWindow; i++ {
		valueNow := t.data[tLen-i-1].wT.(int)
		if valueNow > maxValue {
			maxValue = valueNow
		}
	}
	return maxValue, nil
}
