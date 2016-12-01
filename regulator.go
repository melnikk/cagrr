package cagrr

import (
	"fmt"
	"time"
)

// NewRegulator initializes new stability service object
func NewRegulator(size int) Regulator {
	result := regulator{
		size:   size,
		queues: make(map[string]*Queue),
	}
	return &result
}

// Limit sleeps for measured rate
func (r *regulator) Limit(key string) {
	queue := r.getQueue(key)
	rate := queue.Average()
	log.Debug(fmt.Sprintf("Rate of %s limited to %s", key, rate))
	time.Sleep(rate)
}

func (r *regulator) LimitRateTo(key string, duration time.Duration) {
	queue := r.getQueue(key)
	queue.Pop()
	queue.Push(&QueueNode{duration})
	rate := queue.Average()
	log.Debug(fmt.Sprintf("Duration received: %s [%s, %s]", key, duration, rate))
}

func (r *regulator) getQueue(key string) *Queue {
	queue, exists := r.queues[key]
	if !exists {
		queue = NewQueue(r.size)
		r.queues[key] = queue
	}
	return queue
}
