package repair

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
)

// Runner starts fragment repair
type Runner interface {
	Run(string) error
}

type runner struct {
	host     string
	port     int
	ID       int    `json:"id"`
	Cluster  int    `json:"cluster"`
	Keyspace string `json:"keyspace"`
	Tables   string `json:"tables"`
	Callback string `json:"callback"`
	Start    string `json:"start"`
	End      string `json:"end"`
	Endpoint string `json:"endpoint"`
}

// NewRunner create new runner implementation
func NewRunner(host string, port, cluster int, keyspace, callback string, id int, start, end, endpoint string) Runner {
	result := runner{
		ID:       id,
		host:     host,
		port:     port,
		Cluster:  cluster,
		Keyspace: keyspace,
		Callback: callback,
		Start:    start,
		End:      end,
		Endpoint: endpoint,
	}
	return &result
}

// Run repair in cluster
func (r *runner) Run(tables string) error {
	r.Tables = tables
	TrackRepair(r.Cluster, r.Keyspace, r.Tables, r.ID)

	url := fmt.Sprintf("http://%s:%d/repair/%d", r.host, r.port, r.Cluster)

	log.WithFields(r).Info("Starting repair job")

	buf, err := json.Marshal(r)
	body := bytes.NewBuffer(buf)
	res, err := http.Post(url, "application/json", body)
	if res != nil {
		defer res.Body.Close()
	}
	if err != nil {
		log.WithError(err).Warn("Failed to send repair command")
	}

	response, _ := ioutil.ReadAll(res.Body)
	err = json.Unmarshal(response, r)
	if err != nil {
		log.WithError(err).Warn("Failed to process repair command result")
	}
	return err
}
