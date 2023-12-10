package main

import (
	"os"
	"sync"
)

type DB struct {
	rwLock sync.RWMutex // Allows only one writer at a time
	*dal
}

func Open(path string, options *Options) (*DB, error) {
	options.pageSize = os.Getpagesize()
	dal, err := newDal(path, options)
	if err != nil {
		return nil, err
	}

	db := &DB{
		sync.RWMutex{},
		dal,
	}

	return db, nil
}

func (db *DB) Close() error {
	return db.close()
}

func (db *DB) ReadTx() *tx {
	db.rwLock.RLock()
	return newTx(db, false)
}

func (db *DB) WriteTx() *tx {
	db.rwLock.Lock()
	return newTx(db, true)
}
