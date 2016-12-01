package cagrr

import (
	"fmt"
	"time"
)

func (n *QueueNode) String() string {
	return fmt.Sprint(n.Value)
}

// NewQueue returns a new queue with the given initial size.
func NewQueue(size int) *Queue {
	log.Debug(fmt.Sprintf("Creating new queue of size %d", size))
	return &Queue{
		nodes: make([]*QueueNode, size),
		size:  size,
	}
}

// Push adds a node to the queue.
func (q *Queue) Push(n *QueueNode) {
	if q.head == q.tail && q.count > 0 {
		nodes := make([]*QueueNode, len(q.nodes)+q.size)
		copy(nodes, q.nodes[q.head:])
		copy(nodes[len(q.nodes)-q.head:], q.nodes[:q.head])
		q.head = 0
		q.tail = len(q.nodes)
		q.nodes = nodes
	}
	q.nodes[q.tail] = n
	q.tail = (q.tail + 1) % len(q.nodes)
	q.count++
}

// Pop removes and returns a node from the queue in first to last order.
func (q *Queue) Pop() *QueueNode {
	if q.count == 0 {
		return nil
	}
	node := q.nodes[q.head]
	q.head = (q.head + 1) % len(q.nodes)
	q.count--
	return node
}

// Average counts average queue
func (q *Queue) Average() time.Duration {
	if q.count == 0 {
		return 0
	}
	count := 0
	sum := time.Duration(0)
	for _, node := range q.nodes {
		if nil != node {
			sum = sum + node.Value
			count = count + 1
		}
	}
	return sum / time.Duration(count)
}
