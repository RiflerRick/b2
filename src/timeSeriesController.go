package main

import "errors"

func (t createMetadataTimeSeries) readLatest() (timeSeriesPoint, error) {
	//get the last timeSeriesPoint that was inserted
	createMetadataTimeSeriesMutex.RLock()
	defer createMetadataTimeSeriesMutex.RUnlock()
	if len(t) == 0 {
		return -1, errors.New("length of time series is 0")
	}
	data := t[len(t)-1]
	return data, nil
}

func (t readMetadataTimeSeries) readLatest() (timeSeriesPoint, error) {
	// get the last timeSeriesPoint that was inserted
	readMetadataTimeSeriesMutex.RLock()
	defer readMetadataTimeSeriesMutex.RUnlock()
	if len(t) == 0 {
		return -1, errors.New("length of time series is 0")
	}
	data := t[len(t)-1]
	return data, nil
}

func (t updateMetadataTimeSeries) readLatest() (timeSeriesPoint, error) {
	updateMetadataTimeSeriesMutex.RLock()
	defer updateMetadataTimeSeriesMutex.RUnlock()
	if len(t) == 0 {
		return -1, errors.New("length of time series is 0")
	}
	data := t[len(t)-1]
	return data, nil
}

func (t deleteMetadataTimeSeries) readLatest() (timeSeriesPoint, error) {
	deleteMetadataTimeSeriesMutex.RLock()
	defer deleteMetadataTimeSeriesMutex.RUnlock()
	if len(t) == 0 {
		return -1, errors.New("length of time series is 0")
	}
	data := t[len(t)-1]
	return data, nil
}

func (s *createMetadataTimeSeries) write(t timeSeriesPoint) error {
	createMetadataTimeSeriesMutex.Lock()
	defer createMetadataTimeSeriesMutex.Unlock()
	s = append(s, t)
	return nil
}

func (s *readMetadataTimeSeries) write(t timeSeriesPoint) error {
	readMetadataTimeSeriesMutex.Lock()
	s = append(s, t)
	readMetadataTimeSeriesMutex.Unlock()
	return nil
}

func (s *updateMetadataTimeSeries) write(t timeSeriesPoint) error {
	updateMetadataTimeSeriesMutex.Lock()
	s = append(s, t)
	updateMetadataTimeSeriesMutex.Unlock()
	return nil
}

func (s *deleteMetadataTimeSeries) write(t timeSeriesPoint) error {
	deleteMetadataTimeSeriesMutex.Lock()
	s = append(s, t)
	deleteMetadataTimeSeriesMutex.Unlock()
	return nil
}

func (t createMetadataTimeSeries) getMaxWT(decisionWindow int) (int, error) {
	max_value := 0
	createMetadataTimeSeriesMutex.RLock()
	defer createMetadataTimeSeriesMutex.RUnlock()
	tLen := len(t)
	if tLen == 0 {
		return -1, errors.New("length of time series is 0")
	}
	if tLen < decisionWindow {
		decisionWindow = tLen
	}
	for i := 0; i < decisionWindow; i++ {
		value_now = t[tLen-i-1].wT
		if value_now > max_value {
			max_value = value_now
		}
	}
	return max_value, nil
}

func (t readMetadataTimeSeries) getMaxWT(decisionWindow int) (int, error) {
	max_value := 0
	readMetadataTimeSeriesMutex.RLock()
	defer readMetadataTimeSeriesMutex.RUnlock()
	tLen := len(t)
	if tLen == 0 {
		return -1, errors.New("length of time series is 0")
	}
	if tLen < decisionWindow {
		decisionWindow = tLen
	}
	for i := 0; i < decisionWindow; i++ {
		value_now = t[tLen-i-1].wT
		if value_now > max_value {
			max_value = value_now
		}
	}
	return max_value, nil
}

func (t updateMetadataTimeSeries) getMaxWT(decisionWindow int) int {
	max_value := 0
	updateMetadataTimeSeriesMutex.RLock()
	defer updateMetadataTimeSeriesMutex.RUnlock()
	tLen := len(t)
	if tLen == 0 {
		return -1, errors.New("length of time series is 0")
	}
	if tLen < decisionWindow {
		decisionWindow = tLen
	}
	for i := 0; i < decisionWindow; i++ {
		value_now = t[tLen-i-1].wT
		if value_now > max_value {
			max_value = value_now
		}
	}
	return max_value, nil
}

func (t deleteMetadataTimeSeries) getMaxWT(decisionWindow int) int {
	max_value := 0
	deleteMetadataTimeSeriesMutex.RLock()
	defer deleteMetadataTimeSeriesMutex.RUnlock()
	tLen := len(t)
	if tLen == 0 {
		return -1, errors.New("length of time series is 0")
	}
	if tLen < decisionWindow {
		decisionWindow = tLen
	}
	for i := 0; i < decisionWindow; i++ {
		value_now = t[tLen-i-1].wT
		if value_now > max_value {
			max_value = value_now
		}
	}
	return max_value, nil
}
