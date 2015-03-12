package queue

type Store interface {
	Init() (err error)
	Get() (item Item, err error)
	GetItems(count int) (items []Item, err error)
	Put(item Item) (err error)
	Del(item Item) (err error)
	Size() (n int, err error)
}
