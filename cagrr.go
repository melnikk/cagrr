package cagrr

// Config is a configuration file struct
type Config struct {
	Host     string          `yaml:"host"`
	Port     int             `yaml:"port"`
	Clusters []ClusterConfig `yaml:"clusters"`
}

// ClusterConfig contains configuration of cluster item
type ClusterConfig struct {
	ID        int
	Name      string     `yaml:"name"`
	Interval  string     `yaml:"interval"`
	Keyspaces []Keyspace `yaml:"keyspaces"`
}

// Keyspace contains keyspace repair schedule description
type Keyspace struct {
	Name   string  `yaml:"name"`
	Tables []Table `yaml:"tables"`
}

// Table contains column families to repair
type Table struct {
	Name   string `yaml:"name"`
	Slices int    `yaml:"slices"`
}

// Token represents primary key range
type Token struct {
	ID     string `json:"id"`
	Ranges []Fragment
}

// Fragment of Token range for repair
type Fragment struct {
	ID       int `json:"id"`
	Endpoint string
	Start    string
	End      string
}

// RepairStatus keeps status of repair
type RepairStatus struct {
	ID       int
	Cluster  string
	Keyspace string
	Tables   string
	Count    int
	Duration int
	Error    bool
	Message  string
	Session  string
	Total    int
	Type     string
}

/*
// CompleteRepair updates repair statistics of Ring
func (r *Ring) CompleteRepair(repair *Repair) (int32, int32, int32, time.Duration) {
	repair.Complete()
	//completed := atomic.AddInt32(&r.completed, 1)
	//count := atomic.LoadInt32(&r.count)
	//percent := r.Percent()
	//estimate := r.Estimate(count, completed)
	//return count, completed, percent, estimate
	return 0, 0, 0, 0
}

// Estimate calculates repair completion time
func (r *Ring) Estimate(count, completed int32) time.Duration {
	fragmentLeft := float32(count) - float32(completed)

	worktime := time.Now().Sub(r.started)
	timeLeft := float32(0)

	if completed > 0 {
		timeLeft = float32(worktime) * fragmentLeft / float32(completed)
	}

	return time.Duration(timeLeft)
}

// Percent calculates percent of current repair
func (r *Ring) Percent() int32 {
	//count := atomic.LoadInt32(&r.count)
	//complete := atomic.LoadInt32(&r.completed)
	percent := 0
	//if count > 0 {
	//return complete * 100 / count
	//}
	return int32(percent)
}

// Complete repair of fragment
func (r *Repair) Complete() {
	r.StopMeasure()
}

// Duration measure time of fragment's repair
func (r *Repair) Duration() time.Duration {
	duration := r.T2.Sub(r.T1)
	return duration
}

// StartMeasure fixes start time of Request
func (r *Repair) StartMeasure() {
	r.T1 = time.Now()
}

// StopMeasure fixes end time of Request
func (r *Repair) StopMeasure() {
	r.T2 = time.Now()
}

// Percent returns percent of fragment
func (r *Repair) Percent() int32 {
	return 0
}
*/
