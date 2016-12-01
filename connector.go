package cagrr

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"sort"
)

// GetTokens returns tokens of the Ring
func (c *Connector) GetTokens(cluster, keyspace string, slices int) (TokenSet, error) {
	var tokens TokenSet
	url := fmt.Sprintf("http://%s:%d/ring/%s/describe/%s/%d", c.Host, c.Port, cluster, keyspace, slices)
	res, err := http.Get(url)
	if err != nil {
		log.WithError(err).Error("Failed to obtain ring description")
	}

	if res != nil {
		defer res.Body.Close()
		response, _ := ioutil.ReadAll(res.Body)
		err = json.Unmarshal(response, &tokens)
	}
	return tokens, err
}

// GetTables returns list of column family in clusters keyspace
func (c *Connector) GetTables(cluster, keyspace string, slices int) ([]*Table, error) {
	var names []string
	var result []*Table
	url := fmt.Sprintf("http://%s:%d/tables/%s/%s", c.Host, c.Port, cluster, keyspace)
	log.Debug(fmt.Sprintf("URL: %s", url))

	resp, err := http.Get(url)
	if err != nil {
		log.WithError(err).Error("Failed to obtain column families")
	}

	if resp != nil {
		defer resp.Body.Close()

		response, _ := ioutil.ReadAll(resp.Body)
		err = json.Unmarshal(response, &names)

		if err != nil {
			return nil, err
		}
		result = make([]*Table, 0, len(names))
		sort.Strings(names)

		for i := 0; i < len(names); i++ {
			result = append(result, &Table{Name: names[i], Slices: slices, cluster: cluster, keyspace: keyspace})
		}
	}
	return result, err
}

// Obtain ring fragments
func (c *Connector) Obtain(cluster, keyspace string, slices int) ([]*Fragment, []*Table, error) {
	tokens, err := c.GetTokens(cluster, keyspace, slices)
	tables, err := c.GetTables(cluster, keyspace, slices)

	if err != nil {
		return nil, nil, err
	}
	count := len(tokens) * slices
	frags := make([]*Fragment, 0, count)
	for _, token := range tokens {
		for _, frag := range token.Ranges {

			frag.cluster = cluster
			frag.keyspace = keyspace
			fragLink := frag
			frags = append(frags, &fragLink)
		}
	}
	return frags, tables, nil
}

// RunRepair runs fragment repair
func (c *Connector) RunRepair(repair *Repair) error {
	url := fmt.Sprintf("http://%s:%d/repair", c.Host, c.Port)

	log.WithFields(repair).Info("Starting repair job")

	buf, err := json.Marshal(repair)
	body := bytes.NewBuffer(buf)
	res, err := http.Post(url, "application/json", body)
	if res != nil {
		defer res.Body.Close()
		if res.StatusCode != 200 {
			log.WithError(err).WithFields(repair).Error("Fail to run repair")
		}
	}

	return err

}
