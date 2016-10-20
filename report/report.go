package report

import (
	"github.com/skbkontur/cagrr/http"
	"github.com/skbkontur/cagrr/ops"
)

// Reporter reports about progress
type Reporter interface {
	Report(result http.Status) Reporter
}

type reporter struct {
}

var (
	log ops.Logger
)

// CreateReporter creates reporter of status
func CreateReporter(logger ops.Logger) Reporter {
	log = logger
	rep := reporter{}
	return rep
}

func (r reporter) Report(result http.Status) Reporter {
	log.WithFields(result).Info("Report received")
	return r
}
