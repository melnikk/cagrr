package ops

// Meter measures metric
type Meter interface {
	StartMeasure()
	StopMeasure()
}
