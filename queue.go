package cagrr

import "time"

// NewQueue returns a new queue with the given initial size.
func NewQueue(size int) DurationQueue {
	return &queue{
		nodes: make([]time.Duration, size),
		size:  size,
	}
}

// Average counts average queue
func (q *queue) Average() time.Duration {
	sum := int64(0)
	for _, node := range q.nodes {
		sum = sum + int64(node)
	}
	result := sum / int64(q.size)
	return time.Duration(result)
}

// Len returns actual size of queue
func (q *queue) Len() int {
	return len(q.nodes)
}

// Pop removes and returns a node from the queue in first to last order.
func (q *queue) Pop() time.Duration {
	x := q.nodes[0]
	// Discard top element
	q.nodes = q.nodes[1:]
	return x
}

// Push adds a node to the queue.
func (q *queue) Push(d time.Duration) {
	q.nodes = append(q.nodes, d)
	if len(q.nodes) > q.size {
		q.nodes = q.nodes[1:]
	}
}
