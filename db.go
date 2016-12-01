package cagrr

import (
	"github.com/boltdb/bolt"
)

var (
	database DB
)

// NewDb connects to DB
func NewDb(name string) DB {
	instance := boltDB{}
	var err error
	instance.db, err = bolt.Open(name, 0600, nil)
	if err != nil {
		log.Error(err)
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

// SetDatabase sets package-level DB interface
func SetDatabase(db DB) {
	database = db
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

func getDatabase() DB {
	return database
}
