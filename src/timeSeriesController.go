package main

func (t *createMetadataTimeSeries) readLatest() timeSeriesPoint {
	//get the last timeSeriesPoint that was inserted
	createMetadataTimeSeriesMutex.RLock()
	defer createMetadataTimeSeriesMutex.RUnlock()
	dataNow := t.data
	if len(dataNow) == 0 {
		return timeSeriesPoint{cpm: 0, wT: 0}
	}
	data := t.data[len(dataNow)-1]
	return data
}

func (t *readMetadataTimeSeries) readLatest() timeSeriesPoint {
	// get the last timeSeriesPoint that was inserted
	readMetadataTimeSeriesMutex.RLock()
	defer readMetadataTimeSeriesMutex.RUnlock()
	dataNow := t.data
	if len(dataNow) == 0 {
		return timeSeriesPoint{cpm: 0, wT: 0}
	}
	data := t.data[len(dataNow)-1]
	return data
}

func (t *updateMetadataTimeSeries) readLatest() timeSeriesPoint {
	updateMetadataTimeSeriesMutex.RLock()
	defer updateMetadataTimeSeriesMutex.RUnlock()
	dataNow := t.data
	if len(dataNow) == 0 {
		return timeSeriesPoint{cpm: 0, wT: 0}
	}
	data := t.data[len(dataNow)-1]
	return data
}

func (t *deleteMetadataTimeSeries) readLatest() timeSeriesPoint {
	deleteMetadataTimeSeriesMutex.RLock()
	defer deleteMetadataTimeSeriesMutex.RUnlock()
	dataNow := t.data
	if len(dataNow) == 0 {
		return timeSeriesPoint{cpm: 0, wT: 0}
	}
	data := t.data[len(dataNow)-1]
	return data
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

func (t *createMetadataTimeSeries) getMaxWT(decisionWindow int) int {
	maxValue := 0
	createMetadataTimeSeriesMutex.RLock()
	defer createMetadataTimeSeriesMutex.RUnlock()
	dataNow := t.data
	tLen := len(dataNow)
	if tLen == 0 {
		return 0
	}
	if tLen < decisionWindow {
		decisionWindow = tLen
	}
	for i := 0; i < decisionWindow; i++ {
		valueNow := dataNow[tLen-i-1].wT.(int)
		if valueNow > maxValue {
			maxValue = valueNow
		}
	}
	return maxValue
}

func (t *readMetadataTimeSeries) getMaxWT(decisionWindow int) int {
	maxValue := 0
	readMetadataTimeSeriesMutex.RLock()
	defer readMetadataTimeSeriesMutex.RUnlock()
	dataNow := t.data
	tLen := len(dataNow)
	if tLen == 0 {
		return 0
	}
	if tLen < decisionWindow {
		decisionWindow = tLen
	}
	for i := 0; i < decisionWindow; i++ {
		valueNow := dataNow[tLen-i-1].wT.(int)
		if valueNow > maxValue {
			maxValue = valueNow
		}
	}
	return maxValue
}

func (t *updateMetadataTimeSeries) getMaxWT(decisionWindow int) int {
	maxValue := 0
	updateMetadataTimeSeriesMutex.RLock()
	defer updateMetadataTimeSeriesMutex.RUnlock()
	dataNow := t.data
	tLen := len(dataNow)
	if tLen == 0 {
		return 0
	}
	if tLen < decisionWindow {
		decisionWindow = tLen
	}
	for i := 0; i < decisionWindow; i++ {
		valueNow := dataNow[tLen-i-1].wT.(int)
		if valueNow > maxValue {
			maxValue = valueNow
		}
	}
	return maxValue
}

func (t *deleteMetadataTimeSeries) getMaxWT(decisionWindow int) int {
	maxValue := 0
	deleteMetadataTimeSeriesMutex.RLock()
	defer deleteMetadataTimeSeriesMutex.RUnlock()
	dataNow := t.data
	tLen := len(dataNow)
	if tLen == 0 {
		return 0
	}
	if tLen < decisionWindow {
		decisionWindow = tLen
	}
	for i := 0; i < decisionWindow; i++ {
		valueNow := dataNow[tLen-i-1].wT.(int)
		if valueNow > maxValue {
			maxValue = valueNow
		}
	}
	return maxValue
}
