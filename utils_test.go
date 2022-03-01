// Copyright Kuei-chun Chen, 2022-present. All rights reserved.

package hummingbird

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

func TestToFloat64(t *testing.T) {
	assertEqual(t, float64(123.45), ToFloat64("123.45"))
}

func TestToInt32(t *testing.T) {
	assertEqual(t, int32(123), ToInt32("123"))
}

func TestToInt64(t *testing.T) {
	assertEqual(t, int64(123), ToInt64("123"))
}
