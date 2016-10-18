package report

import (
	"github.com/fatih/structs"
	"github.com/skbkontur/cagrr/http"
	"github.com/skbkontur/cagrr/logs"
)

// Reporter reports about progress
type Reporter interface {
	Report(result http.Status) Reporter
}

type reporter struct {
}

var (
	log logs.Logger
)

// CreateReporter creates reporter of status
func CreateReporter(logger logs.Logger) Reporter {
	log = logger
	rep := reporter{}
	return rep
}

func (r reporter) Report(result http.Status) Reporter {
	log.WithFields(structs.Map(result)).Info("Report received")
	return r
}
