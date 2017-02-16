package cagrr

import "time"

// CheckCompletion of repair
func (t *TrackData) CheckCompletion() {
	t.Percent = t.percent()
	if t.Percent >= 100 {
		t.Completed = true
		t.Started = time.Time{}
		t.Finished = time.Now()
	}
}

// Complete repair fragment
func (t *TrackData) Complete(duration time.Duration) (int, int, time.Duration, float32, time.Duration, time.Duration) {
	start := t.Started
	now := time.Now()

	t.Count++
	t.Duration = now.Sub(start) + duration
	t.Average = t.average()
	t.Estimate = t.estimate(t.Average)

	t.CheckCompletion()

	return t.Total, t.Count, t.Average, t.Percent, t.Estimate, t.Duration
}

// IsRepaired is check for repair completeness
func (t *TrackData) IsRepaired(threshold time.Duration) bool {
	isCompleted := t.Completed
	isScheduled := !t.IsNew()
	isNotSpoiled := !t.IsSpoiled(threshold)
	return isScheduled && isCompleted && isNotSpoiled
}

// IsNew object
func (t *TrackData) IsNew() bool {
	start := t.Started
	return start == time.Time{}
}

// IsSpoiled checks that repair stinks
func (t *TrackData) IsSpoiled(threshold time.Duration) bool {
	now := time.Now()
	start := t.Started
	duration := now.Sub(start)
	return duration > threshold
}

// Restart track
func (t *TrackData) Restart() {
	t.Count = 0
	t.Completed = false
}

// Skip track
func (t *TrackData) Skip() {
	t.Count++
	t.CheckCompletion()
}

// Start track
func (t *TrackData) Start(total int) {
	t.Started = time.Now()
	t.Count = 0
	t.Total = total
	t.Completed = false
}

func (t *TrackData) average() time.Duration {
	average := int64(0)
	if t.Count > 0 {
		average = int64(t.Duration) / int64(t.Count)
	}
	return time.Duration(average)
}

func (t *TrackData) estimate(average time.Duration) time.Duration {
	result := int64(average) * int64(t.Total-t.Count)
	return time.Duration(result)
}

func (t *TrackData) percent() float32 {
	return (100 * float32(t.Count) / float32(t.Total))

}
