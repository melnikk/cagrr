package metrics

import (
	"github.com/skbkontur/cagrr/logs"
)

// Meter measures metric
type Meter interface {
	Measure(metric string, value int) Meter
}

type metrics struct {
}

var (
	meter metrics
	log   *logs.Logger
)

// CreateMeter creates measurer of metrics
func CreateMeter(logger *logs.Logger) Meter {
	log = logger
	meter = metrics{}
	return meter
}

func (m metrics) Measure(metric string, value int) Meter {
	return m
}
