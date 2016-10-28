package ops

import (
	"fmt"
	"time"
)

// Node is Duration struct
type Node struct {
	Value time.Duration
}

func (n *Node) String() string {
	return fmt.Sprint(n.Value)
}

// NewQueue returns a new queue with the given initial size.
func NewQueue(size int) *Queue {
	return &Queue{
		nodes: make([]*Node, size),
		size:  size,
	}
}

// Queue is a basic FIFO queue based on a circular list that resizes as needed.
type Queue struct {
	nodes []*Node
	size  int
	head  int
	tail  int
	count int
}

// Push adds a node to the queue.
func (q *Queue) Push(n *Node) {
	if q.head == q.tail && q.count > 0 {
		nodes := make([]*Node, len(q.nodes)+q.size)
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
func (q *Queue) Pop() *Node {
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

// Cirquit called when isn't breaked
type Cirquit func() error

// CirquitBreaker breaks failed cirquits
type CirquitBreaker interface {
	PassExecution(handler Cirquit) error
}

// Timer checks timeouts in queries
type Timer interface {
	SetTimeout(timeout time.Duration)
	Out() bool
}

// Regulator moderates the process
type Regulator interface {
	CirquitBreaker
	LimitRateTo(time.Duration) Regulator
	Limit()
}

type regulator struct {
	log       Logger
	queue     *Queue
	counter   int
	threshold int
	state     string
	timer     Timer
	rate      time.Duration
	Timeout   time.Duration
}

// NewRegulator initializes new stability service object
func NewRegulator(logger Logger, size int) Regulator {
	result := regulator{
		log:   logger,
		queue: NewQueue(size),
	}
	return &result
}

func (r *regulator) PassExecution(handler Cirquit) error {

	switch {
	case r.state == "semi":
	case r.state == "closed":
		return r.passCirquit(handler)
	case r.state == "open":
		if r.timer.Out() {
			return r.tryToRestore(handler)
		}
	}
	return nil
}

// Limit sleeps for measured rate
func (r *regulator) Limit() {
	r.log.Debug(fmt.Sprintf("Rate limited to %s", r.rate))
	time.Sleep(r.rate)
}

func (r *regulator) LimitRateTo(duration time.Duration) Regulator {
	r.queue.Pop()
	r.queue.Push(&Node{duration})
	r.rate = r.queue.Average()
	r.log.Debug(fmt.Sprintf("Duration received: [%s, %s]", duration, r.rate))
	return r
}

func (r *regulator) resetCounter() {
	r.counter = 0
}

func (r *regulator) incrementAndCheck() {
	r.counter = r.counter + 1
	if r.counter >= r.threshold {
		r.breakCirquit()
	}
}

func (r *regulator) breakCirquit() {
	r.state = "open"
	r.timer.SetTimeout(r.Timeout)
}

func (r *regulator) tryToRestore(handler Cirquit) error {
	r.state = "semi"
	return r.passCirquit(handler)
}
func (r *regulator) restoreCirquit() {
	r.state = "closed"
}

func (r *regulator) passCirquit(handler Cirquit) error {
	err := handler()
	if err == nil {
		r.restoreCirquit()
		r.resetCounter()
	} else {
		r.incrementAndCheck()
	}
	return err
}
