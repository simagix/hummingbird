// Copyright Kuei-chun Chen, 2022-present. All rights reserved.

package humingbird

import (
	"context"
	"strings"

	"github.com/simagix/keyhole/mdb"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

// GetQualifiedDBs returns a list of qualified database names
func GetQualifiedDBs(client *mongo.Client, metaDB string) ([]string, error) {
	var err error
	var ctx = context.Background()
	var dbNames = []string{}
	var dbs mdb.ListDatabases
	if err = client.Database("admin").RunCommand(ctx, bson.D{{Key: "listDatabases", Value: 1}}).Decode(&dbs); err != nil {
		return dbNames, err
	}
	for _, db := range dbs.Databases {
		if db.Name == "admin" || db.Name == "config" || db.Name == "local" || db.Name == metaDB {
			continue
		}
		dbNames = append(dbNames, db.Name)
	}
	return dbNames, nil
}

// GetQualifiedNamespaces returns a list of qualified namespace names
func GetQualifiedNamespaces(client *mongo.Client, includeCollection bool, metaDB string) ([]string, error) {
	var err error
	var ctx = context.Background()
	var dbNames = []string{}
	var namespaces = []string{}

	if dbNames, err = GetQualifiedDBs(client, metaDB); err != nil {
		return namespaces, err
	}
	for _, dbName := range dbNames {
		if !includeCollection {
			namespaces = append(namespaces, dbName+".*")
			continue
		}
		var collNames []string
		if collNames, err = client.Database(dbName).ListCollectionNames(ctx, bson.D{}); err != nil {
			return namespaces, err
		}
		for _, collName := range collNames {
			if strings.HasPrefix(collName, "system.") && collName != "system.js" {
				continue
			} else {
				namespaces = append(namespaces, dbName+"."+collName)
			}
		}
	}
	return namespaces, nil
}
