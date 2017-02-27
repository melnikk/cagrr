package cagrr

import (
	"strings"

	"github.com/hashicorp/consul/api"
)

// NewConsulDb connects to DB
func NewConsulDb() DB {
	instance := consulDB{}
	var err error
	instance.db, err = api.NewClient(api.DefaultConfig())
	if err != nil {
		panic(err)
	}

	return &instance
}

func (r *consulDB) Close() {

}

func (r *consulDB) CreateKey(vars ...string) string {
	return strings.Join(vars, "/")
}

func (r *consulDB) Delete(table, key string) {
	consulKey := strings.Join([]string{table, key}, "/")
	r.db.KV().Delete(consulKey, nil)
}

func (r *consulDB) ReadValue(table, key string) []byte {

	// Get a handle to the KV API
	kv := r.db.KV()
	consulKey := strings.Join([]string{table, key}, "/")
	pair, _, err := kv.Get(consulKey, nil)
	if err != nil {
		panic(err)
	}
	if pair == nil {
		return nil
	}
	return pair.Value
}

func (r *consulDB) WriteValue(table, key string, value []byte) error {

	// Get a handle to the KV API
	kv := r.db.KV()
	consulKey := strings.Join([]string{table, key}, "/")
	// PUT a new KV pair
	p := &api.KVPair{Key: consulKey, Value: value}
	_, err := kv.Put(p, nil)

	return err
}
