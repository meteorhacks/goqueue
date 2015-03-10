package queue

import (
	"errors"
	"time"

	"github.com/satori/go.uuid"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
)

var (
	DefaultSleepTime   = time.Second
	ErrNoOverflowItems = errors.New("no overflow items")
)

type Item struct {
	Key   []byte
	Value []byte
}

type Queue struct {
	DefaultCh  chan Item
	DefaultDB  *leveldb.DB
	OverflowCh chan Item
	OverflowDB *leveldb.DB
	SleepTime  time.Duration
}

func New(size int, defaultDBPath, overflowDBPath string) (q Queue, err error) {
	q = Queue{}
	q.DefaultCh = make(chan Item, size)
	q.OverflowCh = make(chan Item)
	q.SleepTime = DefaultSleepTime

	if q.DefaultDB, err = leveldb.OpenFile(defaultDBPath, nil); err != nil {
		return q, err
	}

	if q.OverflowDB, err = leveldb.OpenFile(overflowDBPath, nil); err != nil {
		return q, err
	}

	q.loadDefaultItems()

	stopRecovery := make(chan bool)
	go q.recoverOverflowItems(stopRecovery)

	return q, nil
}

// data will be sent to the default channel whenever possible
// data will be sent to overflow channel *only* if the buffer is filled
func (q *Queue) Enqueue(value []byte) (err error) {
	key := uuid.NewV4().Bytes()
	item := Item{key, value}

	select {
	case q.DefaultCh <- item:
		return q.DefaultDB.Put(key, value, nil)
	default:
		return q.OverflowDB.Put(key, value, nil)
	}
}

// data will be read from both default channel and overflow channel
// if data is read from overflow channel, it'll be converted into a default
// channel item before returning
func (q *Queue) Dequeue() (item Item, err error) {
	select {
	case item = <-q.DefaultCh:
		return item, nil
	case item = <-q.OverflowCh:
		return item, nil
	}
}

func (q *Queue) Completed(key []byte) (err error) {
	return q.DefaultDB.Delete(key, nil)
}

// load items from DefaultDB to DefaultCh
// this is usually done at program startup
func (q *Queue) loadDefaultItems() (err error) {
	it := q.DefaultDB.NewIterator(nil, nil)
	defer it.Release()

	for it.Next() {
		item := q.getItem(it)
		q.DefaultCh <- item
	}

	if err := it.Error(); err != nil {
		return err
	}

	return nil
}

func (q *Queue) recoverOverflowItems(stop chan bool) (err error) {
	for {
		select {
		case <-stop:
			return nil
		default:
			item, err := q.pickOverflowItem()

			if err == ErrNoOverflowItems {
				time.Sleep(q.SleepTime)
				continue
			}

			if err = q.DefaultDB.Put(item.Key, item.Value, nil); err != nil {
				return err
			}

			if err = q.OverflowDB.Delete(item.Key, nil); err != nil {
				return err
			}

			q.OverflowCh <- item
		}
	}
}

func (q *Queue) pickOverflowItem() (item Item, err error) {
	it := q.OverflowDB.NewIterator(nil, nil)
	defer it.Release()

	if !it.First() {
		return item, ErrNoOverflowItems
	}

	if err := it.Error(); err != nil {
		return item, err
	}

	item = q.getItem(it)

	return item, nil
}

// the iterator Key/Value gets modified in-place when it.Next() is called
// always make a copy of Key and Value before use to keep correct values
func (q *Queue) getItem(it iterator.Iterator) Item {
	itKey := it.Key()
	key := make([]byte, len(itKey))
	copy(key, itKey)

	itValue := it.Value()
	value := make([]byte, len(itValue))
	copy(value, itValue)

	return Item{
		Key:   key,
		Value: value,
	}
}
