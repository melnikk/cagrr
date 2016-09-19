package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
)

// Repair is a Unit of repair work
type Repair struct {
	ID       int64             `json:"id"`
	Keyspace string            `json:"keyspace"`
	Cause    string            `json:"cause"`
	Owner    string            `json:"owner"`
	Options  map[string]string `json:"options"`
	Callback string            `json:"callback"`
	Message  string            `json:"message"`
}

// RepairStatus keeps status of repair
type RepairStatus struct {
	ID      int64  `json:"id"`
	Message string `json:"message"`
	Error   bool   `json:"error"`
	Type    string `json:"type"`
	Count   int    `json:"count"`
	Total   int    `json:"total"`
}

func repairStatus(w http.ResponseWriter, req *http.Request) {
	body, _ := ioutil.ReadAll(req.Body)
	var status RepairStatus
	err := json.Unmarshal(body, &status)
	if err == nil {
		percent := status.Count * 100 / status.Total
		fmt.Printf("\r%d/%d=%d%%", status.Count, status.Total, percent)

	} else {
		fmt.Println(err)
	}
}

func runServer() {
	for {
		http.HandleFunc("/status", repairStatus)
		log.Fatal(http.ListenAndServe("localhost:8000", nil))
	}
}

func main() {
	go runServer()

	options := &Repair{
		Keyspace: "testspace",
		Cause:    "I can",
		Owner:    "miller",
		Callback: "http://localhost:8000/status",
		Options: map[string]string{
			"parallelism": "parallel",
			"ranges":      "3432687330997893542:3451030515182791832",
		},
	}
	buf, _ := json.Marshal(options)
	body := bytes.NewBuffer(buf)

	r, _ := http.Post("http://localhost:8080/repair", "application/json", body)
	response, _ := ioutil.ReadAll(r.Body)
	var repair Repair
	err := json.Unmarshal(response, &repair)
	if err == nil {
		fmt.Println(repair)
	}

}
