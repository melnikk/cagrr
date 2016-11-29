package cagrr

import (
	"fmt"
	"time"
)

// NewRegulator initializes new stability service object
func NewRegulator(size int) Regulator {
	result := regulator{
		queue: NewQueue(size),
	}
	return &result
}

// Limit sleeps for measured rate
func (r *regulator) Limit() {
	log.Debug(fmt.Sprintf("Rate limited to %s", r.rate))
	time.Sleep(r.rate)
}

func (r *regulator) LimitRateTo(duration time.Duration) Regulator {
	r.queue.Pop()
	r.queue.Push(&QueueNode{duration})
	r.rate = r.queue.Average()
	log.Debug(fmt.Sprintf("Duration received: [%s, %s]", duration, r.rate))
	return r
}
