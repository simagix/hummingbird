// Copyright Kuei-chun Chen, 2022-present. All rights reserved.

package humingbird

import (
	"fmt"
	"math"
	"os"
	"strconv"

	"github.com/simagix/keyhole/mdb"
	"go.mongodb.org/mongo-driver/bson"
)

// Stringify returns JSON string
func Stringify(doc interface{}) string {
	data, err := bson.MarshalExtJSON(doc, false, false)
	if err != nil {
		return ""
	}
	return string(data)
}

// DoesFileExist returns true if file exists
func DoesFileExist(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}

// SkipNamespace skips namespace
func SkipNamespace(namespace string, included map[string]*Include) bool {
	if len(included) == 0 {
		return false
	}
	dbName, collName := mdb.SplitNamespace(namespace)
	if included[dbName+".*"] != nil || included[namespace] != nil {
		return false
	}
	if included["*."+collName] != nil || included[namespace] != nil {
		return false
	}
	return true
}

// ToFloat64 converts a value to float64
func ToFloat64(s interface{}) float64 {
	n, err := strconv.ParseFloat(fmt.Sprintf("%v", s), 64)
	if err != nil {
		return math.NaN()
	}
	return n
}

// ToInt32 converts a value to int32
func ToInt32(s interface{}) int32 {
	return int32(ToFloat64(s))
}

// ToInt64 converts a value to int64
func ToInt64(s interface{}) int64 {
	return int64(ToFloat64(s))
}
