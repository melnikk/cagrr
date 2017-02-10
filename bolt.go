package cagrr

import (
	"fmt"
	"strings"

	"github.com/boltdb/bolt"
)

// NewBoltDb connects to DB
func NewBoltDb(name string) DB {
	instance := boltDB{}
	var err error
	instance.db, err = bolt.Open(name, 0600, nil)
	if err != nil {
		log.Error(err)
	}
	instance.CreateTables()
	return &instance
}

func (d *boltDB) Close() {
	d.db.Close()
}

func (d *boltDB) CreateKey(vars ...string) string {
	return strings.Join(vars, "_")
}

// CreateTables initializes tables needed for work
func (d *boltDB) CreateTables() error {
	tx, err := d.db.Begin(true)
	if err != nil {
		log.WithError(err).Warn("Error when starting transaction")
		return err
	}
	defer tx.Rollback()
	/*
		for _, table := range tablesNeeded {
			_, err := tx.CreateBucketIfNotExists([]byte(table))
			if err != nil {
				log.WithError(err).Warn("Error when creating bucket")
				return err
			}
		}

		tx.Commit()
	*/
	return nil
}

func (d *boltDB) ReadOrCreate(table, key string, defaultValue interface{}) ([]byte, bool) {
	val := d.ReadValue(table, key)
	ex := val != nil
	if !ex {
		d.WriteValue(table, key, fmt.Sprintf("%s", defaultValue))
		val = defaultValue.([]byte)
	}
	return val, ex
}

func (d *boltDB) ReadValue(table, key string) []byte {
	var result []byte
	tx, err := d.db.Begin(false)
	if err != nil {
		log.WithError(err).Warn("Error when starting read transaction")
		return []byte(result)
	}
	defer tx.Rollback()

	b := tx.Bucket([]byte(table))
	//if b != nil {
	result = b.Get([]byte(key))
	//}

	return result
}

func (d *boltDB) WriteValue(table, key string, value interface{}) error {
	tx, err := d.db.Begin(true)
	if err != nil {
		log.WithError(err).Warn("Error when starting write transaction")
		return err
	}
	defer tx.Rollback()

	b, _ := tx.CreateBucketIfNotExists([]byte(table))

	b.Put([]byte(key), []byte(value.(string)))

	// Commit the transaction.
	if err := tx.Commit(); err != nil {
		log.WithError(err).Warn("Error when commiting write transaction")
		return err
	}

	return nil
}
