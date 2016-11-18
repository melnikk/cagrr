package repair

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/skbkontur/cagrr"
)

// Obtainer gets info about fragment
type Obtainer interface {
	Obtain(keyspace, callback, cluster string, slices int) ([]Runner, []string, error)
}

type obtainer struct {
	host string
	port int
}

// NewObtainer creates obtainer
func NewObtainer(host string, port int) Obtainer {
	result := obtainer{host, port}
	return &result
}

// Obtain ring
func (o *obtainer) Obtain(keyspace, callback, cluster string, slices int) ([]Runner, []string, error) {
	tokens, err := o.GetTokens(cluster, keyspace, slices)
	tables, err := o.GetTables(cluster, keyspace)

	if err != nil {
		return nil, nil, err
	}
	count := len(tokens) * slices
	repairs := make([]Runner, 0, count)
	for _, token := range tokens {
		for _, frag := range token.Ranges {
			repair := NewRunner(
				o.host,
				o.port,
				cluster,
				keyspace,
				callback,
				frag.ID,
				frag.Start,
				frag.End,
				frag.Endpoint,
			)
			repairs = append(repairs, repair)
		}
	}
	return repairs, tables, nil
}

// TokenSet is a set of Token
type TokenSet []cagrr.Token

// Get tokens of the Ring
func (o *obtainer) GetTokens(cluster string, keyspace string, slices int) (TokenSet, error) {
	var tokens TokenSet
	url := fmt.Sprintf("http://%s:%d/ring/%s/describe/%s/%d", o.host, o.port, cluster, keyspace, slices)
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

func (o *obtainer) GetTables(cluster string, keyspace string) ([]string, error) {
	var names []string
	url := fmt.Sprintf("http://%s:%d/tables/%s/%s", o.host, o.port, cluster, keyspace)
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
