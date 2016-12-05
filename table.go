package cagrr

import (
	"fmt"
	"time"
)

// RegisterStart sets start time of table repair
func (t *Table) RegisterStart(total int32) {
	t.repairs = make(map[int]*Repair)
	t.total = total
	t.started = time.Now()
}

// RegisterFinish sets value of successful table repair
func (t *Table) RegisterFinish() {
	t.started = time.Time{}
}

func (t *Table) completeFragment(id int) int32 {
	log.Debug(fmt.Sprintf("Fragment %d completed", id))
	t.completed++
	return t.completed
}

func (t *Table) estimate() time.Duration {
	fragmentLeft := float32(t.total) - float32(t.completed)

	worktime := time.Now().Sub(t.started)
	timeLeft := float32(0)

	if t.completed > 0 {
		timeLeft = float32(worktime) * fragmentLeft / float32(t.completed)
	}
	return time.Duration(timeLeft)
}

func (t *Table) findRepair(id int) *Repair {
	repair, _ := t.repairs[id]
	return repair
}

func (t *Table) percentage() int32 {
	var percent int32
	if t.total > 0 {
		percent = t.completed * 100 / t.total
	}

	return percent
}
