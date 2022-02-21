// Copyright Kuei-chun Chen, 2022-present. All rights reserved.

package humingbird

import (
	"os"

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
