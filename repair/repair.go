package repair

import (
	"time"

	"github.com/fatih/structs"
	"github.com/skbkontur/cagrr/logs"
)

// Fixer starts repair cycle
type Fixer interface {
	Fix(jobs <-chan Runner)
}

// Runner starts fragment repair
type Runner interface {
	Run() error
	ThenSleep(duration time.Duration) Runner
}

type fixer struct {
}

var (
	log logs.Logger
)

// CreateFixer creates fixer of clusters
func CreateFixer(logger logs.Logger) Fixer {
	log = logger
	fixerImp := fixer{}
	result := Fixer(fixerImp)
	return result
}

func (f fixer) Fix(jobs <-chan Runner) {
	log.Debug("Starting fix loop")
	for job := range jobs {
		err := job.Run()
		if err == nil {
			log.WithFields(structs.Map(job)).Debug("Repair job started")
		} else {
			log.WithError(err).Warn("Failed to start repair")
		}
	}
}
