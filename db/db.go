package db

import (
	"log"

	"github.com/boltdb/bolt"
)

// Connector establish a connection to DB
type Connector interface {
}

// ValueReader reads position data from DB
type ValueReader interface {
	ReadValue(string, string) string
}

// ValueWriter writes position to DB
type ValueWriter interface {
	WriteValue(string, string, string) error
}

// Closer closes DB connection
type Closer interface {
	Close()
}

// DB implements DB interface
type DB interface {
	ValueReader
	ValueWriter
	Closer
}
type boltDB struct {
	db *bolt.DB
}

// NewDb connects to DB
func NewDb(name string) DB {
	instance := boltDB{}
	var err error
	instance.db, err = bolt.Open(name, 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	return &instance
}

func (d *boltDB) Close() {
	d.db.Close()
}

func (d *boltDB) ReadValue(table, key string) string {
	result := make(chan string)
	go d.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(table))

		val := b.Get([]byte(key))
		result <- string(val)
		close(result)
		return nil
	})

	return <-result
}

func (d *boltDB) WriteValue(table, key, value string) error {
	return d.db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(table))
		if err != nil {
			return err
		}
		return b.Put([]byte(key), []byte(value))
	})
}
