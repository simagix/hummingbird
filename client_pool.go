// Copyright Kuei-chun Chen, 2022-present. All rights reserved.

package hummingbird

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/simagix/gox"

	"github.com/simagix/keyhole/mdb"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/x/mongo/driver/connstring"
)

var poolMutex sync.Mutex

type poolMap map[string]*mongo.Client

var pool *poolMap

// GetMongoClient returns a mongo client by a connection string
func GetMongoClient(uri string) (*mongo.Client, error) {
	var err error
	var connstr connstring.ConnString
	poolMutex.Lock()
	defer poolMutex.Unlock()
	if pool == nil {
		pool = &poolMap{}
	}
	if (*pool)[uri] == nil {
		if connstr, err = mdb.ParseURI(uri); err != nil {
			return nil, err
		}
		if (*pool)[uri], err = mdb.NewMongoClient(connstr.String()); err != nil {
			(*pool)[uri] = nil
			return (*pool)[uri], err
		}
	} else {
		if err = (*pool)[uri].Ping(context.Background(), nil); err != nil {
			(*pool)[uri] = nil
			return (*pool)[uri], err
		}
	}
	return (*pool)[uri], nil
}

// GetMongoClientWait waits and returns mongo client
func GetMongoClientWait(connstr string, duration ...time.Duration) (*mongo.Client, error) {
	var err error
	var client *mongo.Client
	timeout := 10 * time.Second
	if len(duration) > 0 {
		timeout = duration[0]
	}
	for i := 0; i < 3; i++ {
		if client, err = GetMongoClient(connstr); err != nil {
			gox.GetLogger("").Error(err)
			time.Sleep(timeout)
			continue
		}
		return client, nil
	}
	return nil, fmt.Errorf("get mongo client timeout")
}
