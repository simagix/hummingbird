// Copyright Kuei-chun Chen, 2022-present. All rights reserved.

package humingbird

import (
	"testing"

	"go.mongodb.org/mongo-driver/bson"
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
