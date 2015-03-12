package queue

import "github.com/satori/go.uuid"

type Item struct {
	ID       []byte `bson:"_id"`
	Payload  []byte `bson:"payload"`
	Overflow bool   `bson:"overflow"`
}

func NewItem(payload []byte) Item {
	key := uuid.NewV4().Bytes()
	return Item{key, payload, false}
}
