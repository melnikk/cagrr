package cagrr

import "time"

// CheckCompletion of repair
func (t *Track) CheckCompletion() {
	t.Percent = t.percent()
	if t.Percent == 100 {
		t.Completed = true
		t.Finished = time.Now()
	}
}

// Complete repair fragment
func (t *Track) Complete(duration time.Duration, err bool) (int, int, time.Duration, float32, time.Duration, time.Duration) {
	start := t.Started
	now := time.Now()

	t.Count++
	if err {
		t.Errors++
	}
	t.Duration = now.Sub(start) + duration
	t.Average = t.average()
	t.Estimate = t.estimate(t.Average)

	t.CheckCompletion()

	return t.Total, t.Count, t.Average, t.Percent, t.Estimate, t.Duration
}

// IsRepaired is check for repair completeness
func (t *Track) IsRepaired(threshold time.Duration) bool {
	return t.Completed && !t.IsSpoiled(threshold)
}

// IsNew object
func (t *Track) IsNew() bool {
	start := t.Started
	return start == time.Time{}
}

// IsSpoiled checks that repair stinks
func (t *Track) IsSpoiled(threshold time.Duration) bool {
	now := time.Now()
	finish := t.Finished
	duration := now.Sub(finish)
	return duration > threshold
}

// Skip track
func (t *Track) Skip() {
	t.Count++
	t.CheckCompletion()
}

// Start track
func (t *Track) Start(total int) {
	t.Started = time.Now()
	t.Count = 0
	t.Errors = 0
	t.Total = total
	t.Completed = false
}

func (t *Track) average() time.Duration {
	average := int64(0)
	if t.Count > 0 {
		average = int64(t.Duration) / int64(t.Count)
	}
	return time.Duration(average)
}

func (t *Track) estimate(average time.Duration) time.Duration {
	result := int64(average) * int64(t.Total-t.Count)
	return time.Duration(result)
}

func (t *Track) percent() float32 {
	return (100 * float32(t.Count) / float32(t.Total))
}
