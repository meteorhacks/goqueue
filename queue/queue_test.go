package queue

import (
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
)

var (
	db1 *leveldb.DB
	db2 *leveldb.DB
)

func Test_Queue_Enqueue_Default(t *testing.T) {
	ResetDBs(t)

	q := Queue{
		DefaultCh:  make(chan Item, 1),
		DefaultDB:  db1,
		OverflowDB: db2,
	}

	value := []byte("test-value")
	q.Enqueue(value)

	if len(q.DefaultCh) != 1 {
		t.Error("Number of items in default channel should be 1")
	}

	db1keys, db1values := AllItems(q.DefaultDB)
	if len(db1keys) != 1 {
		t.Error("Number of items in default db should be 1")
	}

	db2keys, _ := AllItems(q.OverflowDB)
	if len(db2keys) != 0 {
		t.Error("Number of items in overflow db should be 0")
	}

	if string(db1values[0]) != string(value) {
		t.Error("Incorrect value")
	}
}

func Test_Queue_Enqueue_Filled(t *testing.T) {
	ResetDBs(t)

	q := Queue{
		DefaultCh:  make(chan Item),
		DefaultDB:  db1,
		OverflowDB: db2,
	}

	value := []byte("test-value")
	q.Enqueue(value)

	if len(q.DefaultCh) != 0 {
		t.Error("Number of items in default channel should be 0")
	}

	db1keys, _ := AllItems(q.DefaultDB)
	if len(db1keys) != 0 {
		t.Error("Number of items in default db should be 0")
	}

	db2keys, db2values := AllItems(q.OverflowDB)
	if len(db2keys) != 1 {
		t.Error("Number of items in overflow db should be 1")
	}

	if string(db2values[0]) != string(value) {
		t.Error("Incorrect value")
	}
}

func Test_Queue_Dequeue(t *testing.T) {
	ResetDBs(t)

	q := Queue{
		DefaultCh:  make(chan Item, 1),
		OverflowCh: make(chan Item, 1),
	}

	q.DefaultCh <- Item{Key: []byte("k1")}
	q.OverflowCh <- Item{Key: []byte("k2")}

	item1, err := q.Dequeue()
	if err != nil {
		t.Error("Error dequeing item")
	}
	if string(item1.Key) != "k1" && string(item1.Key) != "k2" {
		t.Error("Incorrect value")
	}

	item2, err := q.Dequeue()
	if err != nil {
		t.Error("Error dequeing item")
	}
	if string(item1.Key) != "k1" && string(item1.Key) != "k2" {
		t.Error("Incorrect value")
	}

	if string(item1.Key) == string(item2.Key) {
		t.Error("Both items should be dequeued")
	}
}

func Test_Queue_Completed(t *testing.T) {
	ResetDBs(t)

	q := Queue{
		DefaultDB: db1,
	}

	if err := q.DefaultDB.Put([]byte("k1"), []byte("v1"), nil); err != nil {
		t.Error("Error inserting to DB")
	}

	db1keys, _ := AllItems(db1)
	if len(db1keys) != 1 {
		t.Error("Item should be added to DefaultDB")
	}

	q.Completed([]byte("k1"))

	db1keys, _ = AllItems(db1)
	if len(db1keys) != 0 {
		t.Error("Item should be deleted from DefaultDB")
	}
}

func Test_Queue_loadDefaultItems(t *testing.T) {
	ResetDBs(t)

	q := Queue{
		DefaultCh: make(chan Item, 2),
		DefaultDB: db1,
	}

	if err := q.DefaultDB.Put([]byte("k1"), []byte("v1"), nil); err != nil {
		t.Error("Error inserting to DB")
	}

	if err := q.DefaultDB.Put([]byte("k2"), []byte("v2"), nil); err != nil {
		t.Error("Error inserting to DB")
	}

	if err := q.loadDefaultItems(); err != nil {
		t.Error("Error loading default items")
	}

	item1 := <-q.DefaultCh
	if string(item1.Key) != "k1" || string(item1.Value) != "v1" {
		t.Error("Incorrect value")
	}

	item2 := <-q.DefaultCh
	if string(item2.Key) != "k2" || string(item2.Value) != "v2" {
		t.Error("Incorrect value")
	}

	if reflect.DeepEqual(item1, item2) {
		t.Error("Both items should be loaded")
	}
}

func Test_Queue_recoverOverflowItems(t *testing.T) {
	ResetDBs(t)

	sleep := 100 * time.Millisecond

	q := Queue{
		DefaultDB:  db1,
		OverflowCh: make(chan Item),
		OverflowDB: db2,
		SleepTime:  sleep,
	}

	// start background task
	stopCh := make(chan bool)
	go q.recoverOverflowItems(stopCh)
	time.Sleep(5 * sleep)

	// add some items
	if err := q.OverflowDB.Put([]byte("k1"), []byte("v1"), nil); err != nil {
		t.Error("Error inserting to DB")
	}

	if err := q.OverflowDB.Put([]byte("k2"), []byte("v2"), nil); err != nil {
		t.Error("Error inserting to DB")
	}

	// within next 100ms an item should be moved from OverflowDB to DefaultDB
	// it should remain that way until the item is read form the channel
	time.Sleep(2 * sleep)
	db1keys, _ := AllItems(db1)
	db2keys, _ := AllItems(db2)
	if len(db1keys) != 1 || len(db2keys) != 1 {
		t.Error("Item should be moved from OverflowDB to DefaultDB")
	}

	item1 := <-q.OverflowCh
	if string(item1.Key) != "k1" || string(item1.Value) != "v1" {
		t.Error("Incorrect value")
	}

	time.Sleep(sleep) // give it some time to recover another item

	db1keys, _ = AllItems(db1)
	db2keys, _ = AllItems(db2)
	if len(db1keys) != 2 || len(db2keys) != 0 {
		t.Error("Item should be moved from OverflowDB to DefaultDB")
	}

	item2 := <-q.OverflowCh
	if string(item2.Key) != "k2" || string(item2.Value) != "v2" {
		t.Error("Incorrect value")
	}
}

// -------------------------------------------------------------------------- //

func ResetDBs(t *testing.T) {
	db1 = ResetDB("/tmp/db1", t)
	db2 = ResetDB("/tmp/db2", t)
}

func ResetDB(path string, t *testing.T) *leveldb.DB {
	if err := os.RemoveAll(path); err != nil {
		t.Error("Could not reset " + path)
	}

	if db, err := leveldb.OpenFile(path, nil); err != nil {
		t.Error("Could not create " + path)
	} else {
		return db
	}

	return nil
}

func AllItems(db *leveldb.DB) (keys, values [][]byte) {
	keys = [][]byte{}
	values = [][]byte{}

	it := db.NewIterator(nil, nil)

	for it.Next() {
		keys = append(keys, it.Key())
		values = append(values, it.Value())
	}

	it.Release()

	return keys, values
}
