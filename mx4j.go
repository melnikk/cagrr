package cagrr

import (
	"fmt"
	"sort"
	"strconv"

	"github.com/lytics/gowrapmx4j"
)

var (
	mx4j gowrapmx4j.MX4JService
)

func mx4jTokens(host string, port string) []int64 {
	mx4j = gowrapmx4j.MX4JService{Host: host, Port: port}
	mx4j.Init()
	// Query singlenton values from MX4J

	mm := gowrapmx4j.MX4JMetric{HumanName: "TokenToEndpointMap", ObjectName: "org.apache.cassandra.db:type=StorageService",
		Format: "array", Attribute: "TokenToEndpointMap", ValFunc: gowrapmx4j.DistillAttributeTypes}
	gowrapmx4j.RegistrySet(mm, nil)
	gowrapmx4j.QueryMX4J(mx4j)

	nsb := gowrapmx4j.RegistryGet("TokenToEndpointMap")

	metricMap, _ := gowrapmx4j.DistillAttributeTypes(nsb.Data)

	var tokens []int64
	props := metricMap["TokenToEndpointMap"]
	for token, host := range props.(map[string]interface{}) {
		fmt.Println(fmt.Sprintf("%s -> %s", token, host))
		value, _ := strconv.ParseInt(token, 10, 64)
		tokens = append(tokens, value)
	}
	sort.Sort(BigInts(tokens))
	return tokens
}
