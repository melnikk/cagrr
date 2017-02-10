package cagrr

import (
	"fmt"
	"strings"

	redis "gopkg.in/redis.v5"
)

// NewRedisDb connects to DB
func NewRedisDb(addr, password string, db int) DB {
	instance := &redisDB{}
	instance.db = redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password, // no password set
		DB:       db,       // use default DB
	})

	pong, err := instance.db.Ping().Result()
	fmt.Println(pong, err)

	return instance
}

func (r *redisDB) CreateKey(vars ...string) string {
	return strings.Join(vars, "/")
}

func (r *redisDB) Close() {
	r.db.Close()
}

func (r *redisDB) ReadOrCreate(table, key string, value interface{}) ([]byte, bool) {
	result, _ := r.db.GetSet(key, value).Bytes()
	ex := result != nil
	return result, ex
}

func (r *redisDB) ReadValue(table, key string) []byte {
	result, _ := r.db.Get(key).Bytes()
	return result
}

func (r *redisDB) WriteValue(table, key string, value interface{}) error {
	return nil
}
