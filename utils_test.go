// Copyright Kuei-chun Chen, 2022-present. All rights reserved.

package hummingbird

import (
	"context"
	"fmt"
	"testing"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/x/mongo/driver/uuid"
)

func TestStringify(t *testing.T) {
	str := Stringify("none-exiss")
	assertEqual(t, "", str)

	str = Stringify(bson.M{"hello": "world"})
	assertNotEqual(t, "", str)
}

func TestDoesFileExists(t *testing.T) {
	exists := DoesFileExist("none-exiss")
	assertEqual(t, false, exists)
	exists = DoesFileExist("testdata/minimum.json")
	assertEqual(t, true, exists)
}

func TestToFloat64(t *testing.T) {
	assertEqual(t, float64(123.45), ToFloat64("123.45"))
}

func TestToInt32(t *testing.T) {
	assertEqual(t, int32(123), ToInt32("123"))
}

func TestToInt64(t *testing.T) {
	assertEqual(t, int64(123), ToInt64("123"))
}

// DataGen populate data
func DataGen(coll *mongo.Collection, total int) (*mongo.InsertManyResult, error) {
	ctx := context.Background()
	coll.Drop(ctx)
	var docs []interface{}
	for i := 0; i < total; i++ {
		docs = append(docs, getDoc(i))
	}
	return coll.InsertMany(ctx, docs)
}

// DataGenMulti populate data into different collections
func DataGenMulti(db *mongo.Database, total int, nColls int) error {
	ctx := context.Background()
	db.Drop(ctx)
	docs := map[int][]interface{}{}
	for i := 0; i < total; i++ {
		docs[i%nColls] = append(docs[i%nColls], getDoc(i))
	}
	var colls []*mongo.Collection
	colls = make([]*mongo.Collection, 3)
	for i := 0; i < len(colls); i++ {
		colls[i] = db.Collection(fmt.Sprintf("datagen_%v", i))
		colls[i].InsertMany(ctx, docs[i])

		var session mongo.Session
		var update = bson.M{"$set": bson.M{"birth_year": 1963}}
		client := db.Client()
		var err error
		if session, err = client.StartSession(); err != nil {
			return err
		}
		id1 := bson.D{{"_id", primitive.NewObjectID()}, {"tag", 1}}
		id2 := bson.D{{"_id", primitive.NewObjectID()}, {"tag", 3}}
		ids := []interface{}{id1, id2}
		colls[i].InsertMany(context.Background(), ids)
		if err = session.StartTransaction(); err != nil {
			return err
		}
		if err = mongo.WithSession(ctx, session, func(sc mongo.SessionContext) error {
			if _, err = colls[i].UpdateOne(sc, id1, update); err != nil {
				return err
			}
			if _, err = colls[i].UpdateOne(sc, id2, update); err != nil {
				return err
			}
			if _, err = colls[i].DeleteOne(sc, bson.M{"tag": 1}); err != nil {
				return err
			}
			if _, err = colls[i].DeleteMany(sc, bson.M{"tag": 3}); err != nil {
				return err
			}
			if err = session.CommitTransaction(sc); err != nil {
				return err
			}
			return nil
		}); err != nil {
			return err
		}
		session.EndSession(ctx)

	}
	return nil
}

func getDoc(i int) bson.D {
	n := i + 1001
	num := n*n + Port
	auuid, err := uuid.New()
	if err != nil {
		return bson.D{}
	}
	doc := bson.D{{"_id", primitive.NewObjectID()},
		{"string", fmt.Sprintf("%06d-%v-%v-%v", i+1, num, n, num)},
		{"binary", auuid},
		{"bin1", primitive.Binary{Subtype: byte(1), Data: []byte(auuid[:])}},
		{"bin2", primitive.Binary{Subtype: byte(2), Data: []byte(auuid[:])}},
		{"bin3", primitive.Binary{Subtype: byte(3), Data: []byte(auuid[:])}},
		{"uuid", primitive.Binary{Subtype: byte(4), Data: []byte(auuid[:])}},
		{"int64", int64(num)},
		{"float64", float64(num)},
		{"seq", i},
	}
	var list []int
	for n := 101; n < 110; n++ {
		list = append(list, n*n-n)
	}
	doc = append(doc, bson.D{{"array", list}}...)
	doc = append(doc, bson.D{{"subdoc", bson.D{{"level1", doc}}}}...)
	return doc
}
