package cagrr

// NewFixer creates fixer of clusters
func NewFixer(runner RepairRunner) Fixer {
	result := fixer{runner}
	return &result
}

// Fix repairs from channel
func (f *fixer) Fix(jobs <-chan *Repair) {
	log.WithFields(f).Debug("Starting fix loop")
	for job := range jobs {
		err := f.runner.RunRepair(job)
		if err != nil {
			log.WithError(err).WithFields(job).Warn("Fail to start job")
		}
	}
}
