package ops

import "time"

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
	Timer
}

type regulator struct {
	counter   int
	threshold int
	state     string
	timer     Timer
	Timeout   time.Duration
}

func (r regulator) PassExecution(handler Cirquit) error {

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

func (r regulator) resetCounter() {
	r.counter = 0
}

func (r regulator) incrementAndCheck() {
	r.counter = r.counter + 1
	if r.counter >= r.threshold {
		r.breakCirquit()
	}
}

func (r regulator) breakCirquit() {
	r.state = "open"
	r.timer.SetTimeout(r.Timeout)
}

func (r regulator) tryToRestore(handler Cirquit) error {
	r.state = "semi"
	return r.passCirquit(handler)
}
func (r regulator) restoreCirquit() {
	r.state = "closed"
}

func (r regulator) passCirquit(handler Cirquit) error {
	err := handler()
	if err == nil {
		r.restoreCirquit()
		r.resetCounter()
	} else {
		r.incrementAndCheck()
	}
	return err
}
