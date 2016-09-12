package conn

import (
	"fmt"
	"sort"
	"strconv"

	golokia "github.com/s8sg/go_jolokia"
)

// BigInts are int64 array
type BigInts []int64

func (s BigInts) Len() int {
	return len(s)
}
func (s BigInts) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
func (s BigInts) Less(i, j int) bool {
	return s[i] < s[j]
}

// Runner starts JMX communication
type Runner struct {
	sort.Interface
	url    string
	domain string
	tokens []int64
	client *golokia.JolokiaClient
}

const (
	domainName = "org.apache.cassandra.db"
)

// NewRunner returns new Runner
func NewRunner(host string, port string) Runner {
	jolokia := "jolokia/read"
	client := golokia.NewJolokiaClient("http://" + host + ":" + port + "/" + jolokia)
	client.SetTarget(host + ":" + port)
	result := Runner{domain: domainName, client: client}
	return result
}

// Ring returns directed list of Token
func (r Runner) Ring() []int64 {
	properties := []string{"type=StorageService"}
	attribute := "TokenToEndpointMap"
	props, err := r.client.GetAttr(r.domain, properties, attribute)
	if err != nil {
		fmt.Println(err)
	}
	var tokens []int64
	for token := range props.(map[string]interface{}) {
		value, _ := strconv.ParseInt(token, 10, 64)
		tokens = append(tokens, value)
	}
	sort.Sort(BigInts(tokens))
	r.tokens = tokens
	return r.tokens
}
