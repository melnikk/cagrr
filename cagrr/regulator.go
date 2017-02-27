package cagrr

import "time"

// NewRegulator initializes new stability service object
func NewRegulator(size int) Regulator {
	result := regulator{
		size:   size,
		queues: make(map[string]DurationQueue),
	}
	return &result
}

// Limit sleeps for measured rate
func (r *regulator) Limit(key string) {
	rate := r.Rate(key)
	time.Sleep(rate)
}

func (r *regulator) LimitRateTo(key string, duration time.Duration) time.Duration {
	queue := r.getQueue(key)
	queue.Push(duration)
	result := queue.Average()
	return result
}

func (r *regulator) Rate(key string) time.Duration {
	queue := r.getQueue(key)
	rate := queue.Average()
	return rate
}

func (r *regulator) getQueue(key string) DurationQueue {
	queue, exists := r.queues[key]
	if !exists {
		queue = NewQueue(r.size)
		r.queues[key] = queue
	}
	return queue
}
