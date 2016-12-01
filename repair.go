package cagrr

import (
	"time"
)

// RegisterStart sets start time of fragment repair
func (r *Repair) RegisterStart() {
	r.started = time.Now()
}

// RegisterFinish sets value of successful fragment repair
func (r *Repair) RegisterFinish() time.Duration {
	started := r.started

	finished := time.Now()
	duration := finished.Sub(started)

	return duration
}
