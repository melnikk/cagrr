package repair

import "github.com/skbkontur/cagrr/ops"

// Fixer starts repair cycle
type Fixer interface {
	Fix(jobs <-chan Runner)
}

type fixer struct {
}

var (
	log ops.Logger
)

// CreateFixer creates fixer of clusters
func CreateFixer(logger ops.Logger) Fixer {
	log = logger
	fixerImp := fixer{}
	result := Fixer(&fixerImp)
	return result
}

func (f *fixer) Fix(jobs <-chan Runner) {
	log.Debug("Starting fix loop")
	for job := range jobs {
		err := job.Run()
		if err == nil {
			log.WithFields(job).Debug("Repair job started")
		} else {
			log.WithError(err).Warn("Failed to start repair")
		}
	}
}
