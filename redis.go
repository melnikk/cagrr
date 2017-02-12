package cagrr

import (
	"encoding/json"
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

func (r *redisDB) Delete(table, key string) {
	r.db.Del(key)
}

func (r *redisDB) ReadTrack(table, key string) *TrackData {
	var track TrackData
	value := r.ReadValue(table, key)
	json.Unmarshal(value, &track)
	return &track
}
func (r *redisDB) ReadValue(table, key string) []byte {
	result, _ := r.db.Get(key).Bytes()
	return result
}

func (r *redisDB) WriteTrack(table, key string, value *TrackData) {
	var val []byte
	val, _ = json.Marshal(value)
	r.WriteValue(table, key, val)
}

func (r *redisDB) WriteValue(table, key string, value []byte) error {
	return nil
}
