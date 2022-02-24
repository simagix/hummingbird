// Copyright Kuei-chun Chen, 2022-present. All rights reserved.

package hummingbird

import (
	"context"
	"fmt"
	"strings"

	"go.mongodb.org/mongo-driver/x/mongo/driver/connstring"

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
	if err = client.Database("admin").RunCommand(ctx, bson.D{{"listDatabases", 1}}).Decode(&dbs); err != nil {
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

// GetAllReplicas return all connections strings from an URI
func GetAllReplicas(uri string) ([]string, error) {
	var replicas []string
	cs, err := connstring.Parse(uri)
	if err != nil {
		return replicas, err
	}
	client, err := GetMongoClient(uri)
	if err != nil {
		return replicas, err
	}
	stats := mdb.NewClusterStats("")
	if err = stats.GetClusterStatsSummary(client); err != nil {
		return replicas, err
	}
	if stats.Cluster == mdb.Sharded {
		var shards []mdb.Shard
		if shards, err = mdb.GetShards(client); err != nil {
			return replicas, err
		} else if replicas, err = mdb.GetAllShardURIs(shards, cs); err != nil {
			return replicas, err
		}
	} else if stats.Cluster == mdb.Replica {
		if strings.HasPrefix(cs.String(), "mongodb+srv://") && strings.Contains(cs.String(), ".mongodb.net") {
			replicas = append(replicas, cs.String())
		} else {
			setName := stats.ServerStatus.Repl.SetName
			replicas = append(replicas, addSetName(cs.String(), setName))
		}
	} else {
		replicas = append(replicas, cs.String())
	}

	return replicas, err
}

func addSetName(uri string, setName string) string {
	if !strings.Contains(uri, "replicaSet=") && setName != "" {
		if strings.Contains(uri, "?") {
			uri += fmt.Sprintf("&replicaSet=%v", setName)
		} else {
			uri += fmt.Sprintf("?replicaSet=%v", setName)
		}
		return uri
	}
	return uri
}

// IsBalancerEnabled checks if balancer is enabled
func IsBalancerEnabled(client *mongo.Client) (bool, error) {
	var err error
	var doc bson.M
	if err := client.Database("admin").RunCommand(context.Background(), bson.D{{"balancerStatus", 1}}).Decode(&doc); err != nil {
		return false, err
	}
	if doc != nil && doc["mode"] == "full" {
		return true, err
	}
	return false, err
}
