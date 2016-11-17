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
	LimitRateWith(ops.Regulator) Server
	Serve()
}

type serverMux struct {
	address   string
	mux       *http.ServeMux
	regulator ops.Regulator
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
func (s serverMux) LimitRateWith(regulator ops.Regulator) Server {
	s.regulator = regulator
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
	var status cagrr.RepairStatus
	err := json.Unmarshal(body, &status)
	if err != nil {
		log.WithError(err).Warn(fmt.Sprintf("Invalid status received: %s", string(body)))
	}
	repair.TrackStatus(status)
}
