package http

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/skbkontur/cagrr"
	"github.com/skbkontur/cagrr/ops"
	"github.com/skbkontur/cagrr/repair"
)

// Server serves handlers
type Server interface {
	At(address string) Server
	Using(registrator Registrator) Server
	Through(in chan Status, out chan Status) Server
	Serve()
}

// Obtainer gets info about fragment
type Obtainer interface {
	Obtain(keyspace, callback string, cluster int) ([]repair.Runner, error)
}

// Status wraps parent status struct
type Status cagrr.RepairStatus

// Registrator checks and register status
type Registrator interface {
	RegisterStatus(status Status) (Status, error)
}

// Handler process http-request
type Handler func(w http.ResponseWriter, req *http.Request)

type serverMux struct {
	address     string
	mux         *http.ServeMux
	registrator Registrator
	fails       chan Status
	wins        chan Status
}

var (
	log ops.Logger
)

// CreateServer initializes http listener
func CreateServer(logger ops.Logger) Server {
	log = logger
	server := serverMux{}
	return server
}

func (s serverMux) At(address string) Server {
	s.address = address
	return s
}
func (s serverMux) Using(registrator Registrator) Server {
	s.registrator = registrator
	return s
}
func (s serverMux) Through(wins chan Status, fails chan Status) Server {
	s.wins = wins
	s.fails = fails
	return s
}
func (s serverMux) Serve() {
	for {
		log.Info(fmt.Sprintf("Server listen at %s", s.address))

		s.mux = http.NewServeMux()
		s.mux.Handle("/status", http.HandlerFunc(s.RegisterStatus))
		log.Fatal(http.ListenAndServe(s.address, s.mux))
	}
}

func (s serverMux) RegisterStatus(w http.ResponseWriter, req *http.Request) {
	//log := s.log
	body, _ := ioutil.ReadAll(req.Body)
	var status Status
	var fail error
	err := json.Unmarshal(body, &status)
	if err == nil {
		status, fail = s.CheckStatus(status)

		if fail == nil {
			log.WithFields(status).Debug("Repair suceeded")
			s.wins <- status
		} else {
			log.WithFields(status).Warn("Fragment repair failed")
			s.fails <- status
		}
	} else {
		log.WithError(err).Warn("Invalid status received")
	}
}

// CheckStatus of repair
func (s serverMux) CheckStatus(status Status) (Status, error) {
	//status.Percent = 100
	return status, nil
}
