package queue

import (
	"fmt"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

var (
	MongoStoreDefaultQuery       = bson.M{"overflow": false}
	MongoStoreOverflowQuery      = bson.M{"overflow": true}
	MongoStoreToOverflowModifier = bson.M{"$set": bson.M{"overflow": true}}
	MongoStoreToDefaultModifier  = bson.M{"$set": bson.M{"overflow": false}}
	MongoStoreToDefaultChange    = mgo.Change{
		Update: bson.M{"$set": bson.M{"overflow": false}},
	}
)

type MongoStore struct {
	S *mgo.Session
	C *mgo.Collection
}

func NewMongoStore(session *mgo.Session) *MongoStore {
	s := new(MongoStore)
	s.S = session
	s.C = session.DB("").C("items")
	return s
}

func (s *MongoStore) Init() (err error) {
	// TODO => ensure mongo indexes exists
	return nil
}

func (s *MongoStore) Get() (item Item, err error) {
	cursor := s.C.Find(MongoStoreOverflowQuery)
	info, err := cursor.Apply(MongoStoreToDefaultChange, &item)

	if info == nil || info.Updated == 0 {
		return item, ErrNoOverflowItems
	}

	if err != nil {
		return item, err
	}

	return item, nil
}

func (s *MongoStore) GetItems(count int) (items []Item, err error) {
	_, err = s.C.UpdateAll(MongoStoreDefaultQuery, MongoStoreToOverflowModifier)
	if err != nil {
		// TODO => handle error
		fmt.Println("! err", err)
	}

	it := s.C.Find(MongoStoreDefaultQuery).Limit(count).Iter()
	it.All(&items)

	for _, item := range items {
		if err := s.C.UpdateId(item.ID, MongoStoreToDefaultModifier); err != nil {
			fmt.Println("! err", err)
		}
	}

	return items, nil
}

func (s *MongoStore) Put(item Item) (err error) {
	return s.C.Insert(item)
}

func (s *MongoStore) Del(item Item) (err error) {
	return s.C.RemoveId(item.ID)
}

func (s *MongoStore) Size() (n int, err error) {
	return s.C.Count()
}
