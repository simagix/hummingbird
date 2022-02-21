// Copyright Kuei-chun Chen, 2022-present. All rights reserved.

package humingbird

import (
	"testing"

	"go.mongodb.org/mongo-driver/bson"
)

func TestSet(t *testing.T) {
	includes := &Includes{}
	include := `{ "namespace": "db.collection", "filter": {"a": {"$gt": {"$date": "2020-03-01T00:00:00.001Z"} }} }`
	err := includes.Set(include)
	assertEqual(t, nil, err)
	assertEqual(t, "db.collection", (*includes)[0].Namespace)
	includes.Set(`{ "namespace": "db.another" }`)
	str := includes.String()
	assertNotEqual(t, "", str)
}

func TestGetInclude(t *testing.T) {
	var err error
	var include *Include
	str := `{ "namespace": "db.collection", "filter": {"a": {"$gt": {"$date": "2020-03-01T00:00:00.001Z"} }} }`
	include, err = GetInclude(str)
	assertEqual(t, nil, err)
	assertEqual(t, "db.collection", include.Namespace)

	include, err = GetInclude(`{ "namespace": "" }`)
	assertNotEqual(t, nil, err)

	include, err = GetInclude(`invalid json`)
	assertNotEqual(t, nil, err)

	str = `{ "namespace": "db.collection", "filter": {"a": {"$gt": {"$date": "2020-03-01T00:00:00.001Z"} }}, "masks": ["name"] }`
	include, err = GetInclude(str)
	assertEqual(t, nil, err)
}

func TestConfigureMaskOption(t *testing.T) {
	var err error
	var include Include
	str := `{ "namespace": "db.collection", "filter": {"a": {"$gt": {"$date": "2020-03-01T00:00:00.001Z"} }}, "masks": ["name"] }`
	err = bson.UnmarshalExtJSON([]byte(str), false, &include)
	assertEqual(t, nil, err)
	ConfigureMaskOption(&include)
	assertEqual(t, MaskDefault, include.Method)

	str = `{ "namespace": "db.*", "masks": ["name"] }`
	err = bson.UnmarshalExtJSON([]byte(str), false, &include)
	assertEqual(t, nil, err)
	err = ConfigureMaskOption(&include)
	assertEqual(t, MaskDefault, include.Method)

	str = `{ "namespace": "db.collection", "masks": ["name"], "method": "unknown" }`
	err = bson.UnmarshalExtJSON([]byte(str), false, &include)
	assertEqual(t, nil, err)
	err = ConfigureMaskOption(&include)
	assertNotEqual(t, nil, err)
}
