package core

import (
	"errors"
	"sync"
	"time"

	"github.com/labstack/gommon/log"
)

type InMemoryDbValue struct {
	Value  interface{}
	Expiry *time.Time
	Mu     *sync.RWMutex
}

type InMemoryDb struct {
	Data   map[string]InMemoryDbValue
	Queue  map[string][]interface{}
	cmdMap map[string]func(Operation) (*InMemoryDbValue, error)
}

func (db *InMemoryDb) Set(query Operation) (*InMemoryDbValue, error) {
	done := make(chan int)
	defer func() {
		if err := recover(); err != nil {
			log.Infof("[InMemoryDb] Query timed out: %s", query.QueryString)
		}
		close(done)
	}()

	value := InMemoryDbValue{
		Value:  *query.Value,
		Expiry: nil,
	}

	if query.Expiry != nil {
		exp := time.Now().Add(time.Second * time.Duration(*query.Expiry))
		value.Expiry = &exp
	}

	time.Sleep(time.Second * 15)
	if query.Condition != nil {
		if *query.Condition == NX {
			// NX -> set key value pair only if key does not exist
			if _, ok := db.Data[*query.Key]; !ok {
				db.Data[*query.Key] = value
				return nil, nil
			}
		} else if *query.Condition == XX {
			// XX -> set key value pair only if key exists
			if _, ok := db.Data[*query.Key]; ok {
				db.Data[*query.Key] = value
				return nil, nil
			}
		}
	} else {
		db.Data[*query.Key] = value
	}

	return nil, nil
}

func (db *InMemoryDb) Get(query Operation) (*InMemoryDbValue, error) {
	isExp := db.isKeyExpired(*query.Key)

	if isExp {
		// Passive expired key removal: removes expired keys
		delete(db.Data, *query.Key)
		err := errors.New("key not found")
		return nil, err
	}

	value := db.Data[*query.Key]
	return &value, nil
}
func (db *InMemoryDb) QPush(query Operation) (*InMemoryDbValue, error) {
	key := *query.Key
	queueValues := make([]interface{}, len(query.QueueValues))
	for i, value := range query.QueueValues {
		queueValues[i] = value
	}

	db.Queue[key] = append(db.Queue[key], queueValues...)
	return nil, nil
}

func (db *InMemoryDb) QPop(query Operation) (*InMemoryDbValue, error) {
	key := *query.Key
	queueValues, ok := db.Queue[key]
	if !ok || len(queueValues) == 0 {
		err := errors.New("queue is empty or not found")
		return nil, err
	}

	poppedValue := queueValues[0]
	db.Queue[key] = queueValues[1:]

	return &InMemoryDbValue{Value: poppedValue}, nil
}

func (db *InMemoryDb) isKeyExpired(key string) bool {
	if val, ok := db.Data[key]; ok {
		if val.Expiry == nil {
			return false
		}

		exp := (*val.Expiry).Unix() - time.Now().Unix()
		if exp > 0 {
			return false
		}
	}

	return true
}

func (db *InMemoryDb) Command(commandString string) (interface{}, error) {
	pr := NewCommandParser(commandString)
	pr.Parse()
	if err := pr.Err(); err != nil {
		return nil, err
	}

	// Check if command is valid
	if !pr.IsValid() {
		err := errors.New("invalid command")
		return nil, err
	}

	// Run query
	cmd := *pr.Query.Cmd
	dbResponse, err := db.cmdMap[cmd](pr.Query)
	if err != nil {
		return nil, err
	}

	log.Infof("[InMemoryDb] Query Executed %s", commandString)
	if dbResponse != nil {
		return dbResponse.Value, nil
	}
	return nil, nil
}

func StartInMemoryDb() *InMemoryDb {
	log.Infof("[InMemoryDb] Initiating startup...")
	db := &InMemoryDb{
		Data:  map[string]InMemoryDbValue{},
		Queue: make(map[string][]interface{}, 0),
	}

	// Initialize cmdMap separately
	cmdMap := map[string]func(Operation) (*InMemoryDbValue, error){
		"Set":   db.Set,
		"Get":   db.Get,
		"QPUSH": db.QPush,
		"QPOP":  db.QPop,
	}

	db.cmdMap = cmdMap

	log.Infof("[InMemoryDb] started inmemory database")

	return db
}
