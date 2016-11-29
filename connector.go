package cagrr

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
)

// GetTokens returns tokens of the Ring
func (c *Connector) GetTokens(cluster, keyspace string, slices int) (TokenSet, error) {
	var tokens TokenSet
	url := fmt.Sprintf("http://%s:%d/ring/%s/describe/%s/%d", c.Host, c.Port, cluster, keyspace, slices)
	res, err := http.Get(url)

	if res != nil {
		defer res.Body.Close()
	}
	if err != nil {
		log.WithError(err).Warn("Failed to obtain ring description")
	}

	response, _ := ioutil.ReadAll(res.Body)
	err = json.Unmarshal(response, &tokens)
	return tokens, err
}

// GetTables returns list of column family in clusters keyspace
func (c *Connector) GetTables(cluster, keyspace string) ([]string, error) {
	var names []string
	url := fmt.Sprintf("http://%s:%d/tables/%s/%s", c.Host, c.Port, cluster, keyspace)
	log.Debug(fmt.Sprintf("URL: %s", url))

	res, err := http.Get(url)

	if res != nil {
		defer res.Body.Close()
	}
	if err != nil {
		log.WithError(err).Warn("Failed to obtain column families")
	}

	response, _ := ioutil.ReadAll(res.Body)
	err = json.Unmarshal(response, &names)
	return names, err
}

// Obtain ring fragments
func (c *Connector) Obtain(cluster, keyspace string, slices int) ([]Fragment, []string, error) {
	tokens, err := c.GetTokens(cluster, keyspace, slices)
	tables, err := c.GetTables(cluster, keyspace)

	if err != nil {
		return nil, nil, err
	}
	count := len(tokens) * slices
	frags := make([]Fragment, 0, count)
	for _, token := range tokens {
		for _, frag := range token.Ranges {
			frags = append(frags, frag)
		}
	}
	return frags, tables, nil
}

// RunRepair runs fragment repair
func (c *Connector) RunRepair(repair Repair) error {
	url := fmt.Sprintf("http://%s:%d/repair", c.Host, c.Port)

	log.WithFields(repair).Info("Starting repair job")

	buf, err := json.Marshal(&repair)
	body := bytes.NewBuffer(buf)
	res, err := http.Post(url, "application/json", body)
	if res != nil {
		defer res.Body.Close()
	}
	return err

}
