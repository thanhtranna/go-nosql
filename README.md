# GoNoSQL

GoNoSQL is a simple, persistent key/value store written in pure Go. The project aims to provide a working yet simple
example of a working database. If you're interested in databases, I encourage you to start here.

This database accompanies  my [blog post](https://betterprogramming.pub/build-a-nosql-database-from-the-scratch-in-1000-lines-of-code-8ed1c15ed924) on how to write a database from scratch.

## Installing

To start using GoNoSQL, install Go and run `go get`:

```sh
go get -u github.com/thanhtranna/go-nosql
```

## Basic usage
```go
package main

import (
    gonosql "github.com/thanhtranna/go-nosql"
)

func main() {
	path := "nosql.db"
	db, _ := gonosql.Open(path, gonosql.DefaultOptions)

	tx := db.WriteTx()
	name := []byte("test")
	collection, _ := tx.CreateCollection(name)

	key, value := []byte("key1"), []byte("value1")
	_ = collection.Put(key, value)

	_ = tx.Commit()
}

## Transactions
Read-only and read-write transactions are supported. LibraDB allows multiple read transactions or one read-write 
transaction at the same time. Transactions are goroutine-safe.

LibraDB has an isolation level: [Serializable](https://en.wikipedia.org/wiki/Isolation_(database_systems)#Serializable).
In simpler words, transactions are executed one after another and not at the same time.This is the highest isolation level.

### Read-write transactions

```go
tx := db.WriteTx()
...
if err := tx.Commit(); err != nil {
    return err
}
```
### Read-only transactions
```go
tx := db.ReadTx()
...
if err := tx.Commit(); err != nil {
    return err
}
```

## Collections
Collections are a grouping of key-value pairs. Collections are used to organize and quickly access data as each
collection is B-Tree by itself. All keys in a collection must be unique.
```go
tx := db.WriteTx()
collection, err := tx.CreateCollection([]byte("test"))
if err != nil {
	return err
}
_ = tx.Commit()
```

### Auto generating ID
The `Collection.ID()` function returns an integer to be used as a unique identifier for key/value pairs.
```go
tx := db.WriteTx()
collection, err := tx.GetCollection([]byte("test"))
if err != nil {
    return err
}
id := collection.ID()
_ = tx.Commit()
```
## Key-Value Pairs
Key/value pairs reside inside collections. CRUD operations are possible using the methods `Collection.Put` 
`Collection.Find` `Collection.Remove` as shown below.   
```go
tx := db.WriteTx()
collection, err := tx.GetCollection([]byte("test"))
if  err != nil {
    return err
}

key, value := []byte("key1"), []byte("value1")
if err := collection.Put(key, value); err != nil {
    return err
}
if item, err := collection.Find(key); err != nil {
    return err
}

if err := collection.Remove(key); err != nil {
    return err
}
_ = tx.Commit()
```

## How to run unit test

```
go test ./...
```