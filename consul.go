package cagrr

import (
	"fmt"
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

func (r *consulDB) ReadOrCreate(table, key string, value interface{}) ([]byte, bool) {
	val := []byte(fmt.Sprintf("%s", value))
	kv := r.db.KV()
	pair, _, err := kv.Get(key, nil)
	if err != nil {
		panic(err)
	}
	ex := pair != nil
	if !ex {
		r.WriteValue(table, key, val)
	} else {
		val = pair.Value
	}
	return val, ex
}

func (r *consulDB) ReadValue(table, key string) []byte {

	// Get a handle to the KV API
	kv := r.db.KV()
	consulKey := strings.Join([]string{table, key}, "/")
	pair, _, err := kv.Get(consulKey, nil)
	if err != nil {
		panic(err)
	}
	return pair.Value
}

func (r *consulDB) WriteValue(table, key string, value interface{}) error {

	// Get a handle to the KV API
	kv := r.db.KV()
	consulKey := strings.Join([]string{table, key}, "/")
	// PUT a new KV pair
	p := &api.KVPair{Key: consulKey, Value: []byte(fmt.Sprintf("%s", value))}
	_, err := kv.Put(p, nil)

	return err
}
