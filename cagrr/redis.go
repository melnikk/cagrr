package cagrr

import (
	"strings"
	"time"

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
	log.WithError(err).Info(pong)
	return instance
}

func (r *redisDB) CreateKey(vars ...string) string {
	return strings.Join(vars, "/")
}

func (r *redisDB) Close() {
	r.db.Close()
}

func (r *redisDB) Delete(table, key string) {
	r.db.Del(key)
}

func (r *redisDB) ReadValue(table, key string) []byte {
	result, _ := r.db.Get(key).Bytes()
	return result
}

func (r *redisDB) WriteValue(table, key string, value []byte) error {
	status := r.db.Set(key, string(value), time.Second)
	return status.Err()
}
