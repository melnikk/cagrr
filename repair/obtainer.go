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
	Obtain(keyspace, callback string, cluster, slices int) ([]Runner, error)
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
func (o *obtainer) Obtain(keyspace, callback string, cluster, slices int) ([]Runner, error) {
	tokens, err := o.Get(cluster, keyspace, slices)

	if err != nil {
		return nil, err
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
	return repairs, nil
}

// TokenSet is a set of Token
type TokenSet []cagrr.Token

// Get the Ring
func (o *obtainer) Get(cluster int, keyspace string, slices int) (TokenSet, error) {
	var tokens TokenSet
	url := fmt.Sprintf("http://%s:%d/ring/%d/describe/%s/%d", o.host, o.port, cluster, keyspace, slices)
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
