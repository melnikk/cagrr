package cagrr

import (
	"fmt"

	golokia "github.com/s8sg/go_jolokia"
)

// Runner starts JMX communication
type Runner struct {
	url    string
	domain string
	client *golokia.JolokiaClient
}

const (
	domainName = "org.apache.cassandra.db"
)

// NewRunner returns new Runner
func NewRunner(host string, port string) Runner {
	jolokia := "cassandra.jmx"
	client := golokia.NewJolokiaClient("http://" + host + ":" + port + "/" + jolokia)
	client.SetTarget(host + ":" + port)
	result := Runner{domain: domainName, client: client}
	return result
}

// Ring returns directed list of Token
func (r Runner) Ring() {
	properties := []string{"type=StorageService"}
	props, err := r.client.ListProperties(r.domain, properties)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(props)
}
