package queue

import "errors"

var (
	ErrNoItems         = errors.New("no items")
	ErrNoOverflowItems = errors.New("no overflow items")
)

type Queue struct {
	S    Store
	C    chan Item
	size int
}

func NewQueue(store Store, size int) Queue {
	return Queue{
		S:    store,
		C:    make(chan Item, size),
		size: size,
	}
}

func (q *Queue) Init() (err error) {
	q.S.Init()

	items, err := q.S.GetItems(q.size)
	if err != nil {
		return err
	}

	for _, item := range items {
		q.C <- item
	}

	return nil
}

func (q *Queue) Enqueue(payload []byte) (err error) {
	item := NewItem(payload)

	select {
	case q.C <- item:
	default:
		item.Overflow = true
	}

	return q.S.Put(item)
}

func (q *Queue) Dequeue() (item Item, err error) {
	select {
	case item = <-q.C:
	default:
		item, err = q.S.Get()

		if err == ErrNoOverflowItems {
			return item, ErrNoItems
		} else {
			return item, err
		}
	}

	return item, err
}

func (q *Queue) Remove(item Item) (err error) {
	return q.S.Del(item)
}

func (q *Queue) Size() (n int, err error) {
	return q.S.Size()
}
