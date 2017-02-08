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

// ObtainTables returns list of column family in clusters keyspace
func (c *Connector) ObtainTables(cluster, keyspace string) ([]*Table, error) {
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
		err = json.Unmarshal(response, &result)

		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

// ObtainFragments returns ring slices
func (c *Connector) ObtainFragments(cluster, keyspace string, slices int) ([]*Fragment, error) {
	tokens, err := c.GetTokens(cluster, keyspace, slices)
	if err != nil {
		log.WithError(err).Error("Token obtain error")
		return nil, err
	}

	count := len(tokens) * slices
	frags := make([]*Fragment, 0, count)
	for _, token := range tokens {
		for _, frag := range token.Ranges {
			fragLink := frag
			frags = append(frags, &fragLink)
		}
	}
	return frags, nil
}

// RunRepair runs fragment repair
func (c *Connector) RunRepair(repair *Repair) error {
	url := fmt.Sprintf("http://%s:%d/repair", c.Host, c.Port)

	log.WithFields(repair).Info("Starting repair job")

	buf, _ := json.Marshal(repair)
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
