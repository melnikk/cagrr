package ops

import "time"

// Cirquit called when isn't breaked
type Cirquit func() error

// CirquitBreaker breaks failed cirquits
type CirquitBreaker interface {
	Pass(handler Cirquit) error
}

// Timer checks timeouts in queries
type Timer interface {
	Set(timeout time.Duration)
	Out() bool
}

type breaker struct {
	counter   int
	threshold int
	state     string
	timer     Timer
	Timeout   time.Duration
}

func (b breaker) Pass(handler Cirquit) error {

	switch {
	case b.state == "semi":
	case b.state == "closed":
		return b.passCirquit(handler)
	case b.state == "open":
		if b.timer.Out() {
			return b.tryToRestore(handler)
		}
	}
	return nil
}

func (b breaker) resetCounter() {
	b.counter = 0
}

func (b breaker) incrementAndCheck() {
	b.counter = b.counter + 1
	if b.counter >= b.threshold {
		b.breakCirquit()
	}
}

func (b breaker) breakCirquit() {
	b.state = "open"
	b.timer.Set(b.Timeout)
}

func (b breaker) tryToRestore(handler Cirquit) error {
	b.state = "semi"
	return b.passCirquit(handler)
}
func (b breaker) restoreCirquit() {
	b.state = "closed"
}

func (b breaker) passCirquit(handler Cirquit) error {
	err := handler()
	if err == nil {
		b.restoreCirquit()
		b.resetCounter()
	} else {
		b.incrementAndCheck()
	}
	return err
}
